import { groupBy, sumBy } from "lodash";
import { PROTOCOL } from "./indexer";
import { prisma } from "../../services/db";
import { SDK, cetusConfig } from "./init_mainnet";
import { suiClient } from "../../services/client";
import { CLMM, Position, SuiContext, TokenState } from "../../interface";
import {
  ClmmPoolUtil,
  CollectRewarderParams,
  TickMath,
} from "@cetusprotocol/cetus-sui-clmm-sdk";
import BN from "bn.js";
import { INimbusTokenPrice, getNimbusDBPrice, valueConvert } from "../../utils";
import { getUserContext } from "../context";
import { TransactionBlock } from "@mysten/sui.js/transactions";

const toTokenState = (
  balance: string | number,
  price: INimbusTokenPrice
): TokenState => {
  const amount = Number(valueConvert(balance, price.decimals || 9));
  return {
    amount,
    value: amount * price.price,
    token: price,
  };
};

const PACKAGE =
  "0x11ea791d82b5742cc8cab0bf7946035c97d9001d7c3803a93f119753da66f526";

export const getUserLPPositions = async (
  owner: string,
  { balances, ownedObj }: SuiContext
): Promise<CLMM[]> => {
  // const allInputs = await prisma.defi_clmm_lp.findMany({
  //   where: {
  //     owner,
  //     chain: "SUI",
  //     protocol: PROTOCOL,
  //   },
  //   orderBy: {
  //     timestamp: "asc",
  //   },
  // });

  const allInputs = [];

  const fees = allInputs.filter((item) => item.action === "Fee");
  const lpChanges = allInputs.filter((item) =>
    ["Add", "Remove"].includes(item.action)
  );
  const closes = allInputs.filter((item) => item.action === "Close");

  const normalPositions = await SDK.Position.getPositionList(owner); // Normal LP
  const lpStacked = ownedObj.filter(
    (item) => item?.data?.type === `${PACKAGE}::pool::WrappedPositionNFT`
  );

  const lpRewards = await Promise.all(
    lpStacked.map(async (lp) => {
      const txb = new TransactionBlock();
      txb.moveCall({
        target: `${PACKAGE}::router::accumulated_position_rewards`,
        typeArguments: [],
        arguments: [
          txb.object(cetusConfig.global_config_id),
          txb.object(cetusConfig.rewarder_manager_id),
          txb.object(lp.data?.content.fields?.pool_id as string),
          txb.pure(lp.data?.objectId, "address"),
          txb.object(cetusConfig.time_package),
        ],
      });

      // const dev = await SDK.fullClient.sendSimulationTransaction(txb, owner);
      const dev = await suiClient.devInspectTransactionBlock({
        sender: owner,
        transactionBlock: txb,
      });

      const rewards = dev.events.filter((item) =>
        item.type.endsWith("AccumulatedPositionRewardsEvent")
      );
      return rewards
        .map((event) =>
          event.parsedJson.rewards?.contents.map((item) => {
            return {
              posId: lp.data?.objectId,
              coin_address: "0x" + item.key.name,
              amount_owed: item.value,
            };
          })
        )
        .flat();
    })
  );

  const lpStackedRewardByPos = groupBy(lpRewards.flat(), (item) => item.posId);

  const listPositions = [
    ...normalPositions,
    ...lpStacked.map((item) => {
      return {
        pos_object_id: item.data?.objectId,
        liquidity: item.data?.content?.fields?.clmm_postion?.fields?.liquidity,
        pool: item.data?.content?.fields?.clmm_postion.fields?.pool,
        fee_owed_a: 0, // TODO:
        fee_owed_b: 0, // TODO:
        tick_lower_index:
          item.data?.content?.fields?.clmm_postion.fields?.tick_lower_index
            ?.fields?.bits,
        tick_upper_index:
          item.data?.content?.fields?.clmm_postion.fields?.tick_upper_index
            ?.fields?.bits,
        coin_type_a:
          item.data?.content?.fields?.clmm_postion.fields?.coin_type_a?.fields
            ?.name,
        coin_type_b:
          item.data?.content?.fields?.clmm_postion.fields?.coin_type_b?.fields
            ?.name,
        yield: lpStackedRewardByPos[item.data?.objectId],
        isStaking: true,
      };
    }),
  ];

  const rewards = await SDK.Rewarder.batchFetchPositionRewarders(
    normalPositions.map((item) => item.pos_object_id)
  );

  const lpChangesByPositions = groupBy(lpChanges, (item) => item.position);
  const lpCurrent = await Promise.all(
    listPositions.map(async (position) => {
      const positionData = lpChangesByPositions[position.pos_object_id] || [];
      const feeData = fees.filter(
        (item) => item.position === position.pos_object_id
      );
      const addTxs = positionData.filter((item) => item.action === "Add");
      const removeTxs = positionData.filter((item) => item.action === "Remove");

      const poolData = await SDK.Pool.getPool(position.pool);

      const lowerSqrtPrice = TickMath.tickIndexToSqrtPriceX64(
        position.tick_lower_index
      );
      const upperSqrtPrice = TickMath.tickIndexToSqrtPriceX64(
        position.tick_upper_index
      );

      const liquidity = new BN(position?.liquidity || 0);
      const curSqrtPrice = new BN(poolData.current_sqrt_price);

      const { coinA, coinB } = ClmmPoolUtil.getCoinAmountFromLiquidity(
        liquidity,
        curSqrtPrice,
        lowerSqrtPrice,
        upperSqrtPrice,
        false
      );

      const reward = rewards[position.pos_object_id] || [];
      const lpReward = lpStackedRewardByPos[position.pos_object_id] || [];
      const [rewardsPrices, lpRewardsPrices, [tokenAPrice, tokenBPrice]] =
        await Promise.all([
          Promise.all(
            reward.map((item) => getNimbusDBPrice(item.coin_address, "SUI"))
          ),
          Promise.all(
            lpReward.map((item) => getNimbusDBPrice(item.coin_address, "SUI"))
          ),
          Promise.all([
            getNimbusDBPrice("0x" + position.coin_type_a, "SUI"),
            getNimbusDBPrice("0x" + position.coin_type_b, "SUI"),
          ]),
        ]);

      return {
        positionId: position.pos_object_id,
        owner: owner,
        input: [
          {
            amount:
              sumBy(addTxs, (item) => item.token_a_quality) -
              sumBy(removeTxs, (item) => item.token_a_quality),
            value:
              sumBy(
                addTxs,
                (item) => item.token_a_quality * item.token_a_price
              ) -
              sumBy(
                removeTxs,
                (item) => item.token_a_quality * item.token_a_price
              ),
            token: tokenAPrice,
          },
          {
            amount:
              sumBy(addTxs, (item) => item.token_b_quality) -
              sumBy(removeTxs, (item) => item.token_b_quality),
            value:
              sumBy(
                addTxs,
                (item) => item.token_b_quality * item.token_b_price
              ) -
              sumBy(
                removeTxs,
                (item) => item.token_b_quality * item.token_b_price
              ),
            token: tokenBPrice,
          },
        ],
        yieldCollected: [
          // TODO: Add stake collected
          {
            amount: sumBy(feeData, (item) => item.token_a_quality),
            value: sumBy(
              feeData,
              (item) => item.token_a_quality * item.token_a_price
            ),
            token: tokenAPrice,
          },
          {
            amount: sumBy(feeData, (item) => item.token_b_quality),
            value: sumBy(
              feeData,
              (item) => item.token_b_quality * item.token_b_price
            ),
            token: tokenBPrice,
          },
        ],
        current: {
          tokens: [
            toTokenState(coinA?.toString() || 0, tokenAPrice),
            toTokenState(coinB?.toString() || 0, tokenBPrice),
          ],
          currentPrice: TickMath.tickIndexToPrice(
            poolData.current_tick_index,
            tokenAPrice.decimals || 9,
            tokenBPrice.decimals || 9
          ),
          lowerPrice: TickMath.tickIndexToPrice(
            position.tick_lower_index,
            tokenAPrice.decimals || 9,
            tokenBPrice.decimals || 9
          ),
          upperPrice: TickMath.tickIndexToPrice(
            position.tick_upper_index,
            tokenAPrice.decimals || 9,
            tokenBPrice.decimals || 9
          ),
          isInRange:
            curSqrtPrice >= lowerSqrtPrice && curSqrtPrice <= upperSqrtPrice,
          yield: [
            toTokenState(position?.fee_owed_a?.toString() || 0, tokenAPrice),
            toTokenState(position?.fee_owed_b?.toString() || 0, tokenBPrice),
            ...reward.map((item, index) => {
              const price = rewardsPrices[index];

              return toTokenState(item.amount_owed.toString(), price);
            }),
            ...lpReward.map((item, index) => {
              const price = lpRewardsPrices[index];

              return toTokenState(item.amount_owed.toString(), price);
            }),
          ],
        },
        fee: {
          value: sumBy(positionData, (item) => item.fee),
        },
        tags: position?.isStaking ? ["Farming"] : [],
        chain: "SUI",
        type: "CLMM",
        meta: {
          protocol: {
            name: PROTOCOL,
            logo: "",
            url: "",
          },
          url: "", // TODO:
        },
      };
    })
  );

  return lpCurrent;
};

// async getVeNFTDividendInfo(t, e) {
//   try {
//       const n = await this._sdk.ClmmSDK.fullClient.getDynamicFieldObject({
//           parentId: t,
//           name: {
//               type: "0x2::object::ID",
//               value: e
//           }
//       })
//         , i = Qe(n);
//       return xo.buildVeNFTDividendInfo(i)
//   } catch {
//       return
//   }
// }

// buildVeNFTDividendInfo(t) {
//   const e = {
//       id: t.id.id,
//       ve_nft_id: t.name,
//       rewards: []
//   };
//   return t.value.fields.value.fields.dividends.fields.contents.forEach(n=>{
//       const i = [];
//       n.fields.value.fields.contents.forEach(r=>{
//           i.push({
//               coin_type: ce(r.fields.key.fields.name).source_address,
//               amount: r.fields.value
//           })
//       }
//       ),
//       e.rewards.push({
//           period: Number(n.fields.key),
//           rewards: i
//       })
//   }
//   ),
//   e
// }

// async getDividendManager(t=!1) {
//   const {dividend_manager_id: e} = L(this._sdk.sdkOptions.xcetus_dividends)
//     , n = `${e}_getDividendManager`
//     , i = this.getCache(n, t);
//   if (i !== void 0)
//       return i;
//   const r = await this._sdk.ClmmSDK.fullClient.getObject({
//       id: e,
//       options: {
//           showContent: !0
//       }
//   })
//     , s = Qe(r)
//     , o = Eo.buildDividendManager(s);
//   return this.updateCache(n, o, Oe),
//   o
// }

const getUserStakingPositions = async (owner: string, context: SuiContext) => {
  const lockedCetus = context.ownedObj.filter(
    (item) =>
      item.data?.type ===
      "0x9e69acc50ca03bc943c4f7c5304c2a6002d507b51c11913b247159c60422c606::xcetus::VeNFT"
  );

  const xCetusPrice = await getNimbusDBPrice(
    "0x6864a6f921804860930db6ddbe2e16acdf8504495ea7481637a1c8b9a8fe54b::cetus::CETUS",
    "SUI"
  );

  const result = await Promise.all(
    lockedCetus.map((item) => {
      const balance = item.data?.content.fields?.xcetus_balance;

      const amount = Number(valueConvert(balance, 9));

      return {
        positionId: "xCetus",
        type: "Stake",
        owner,
        input: [
          {
            amount: amount,
            value: 0,
            token: xCetusPrice,
          },
        ],
        yieldCollected: [],
        current: {
          tokens: [
            {
              amount: amount,
              value: amount * xCetusPrice.price,
              token: xCetusPrice,
            },
          ],
          yield: [],
        },
        fee: {
          value: 0,
        },
        chain: "SUI",
        meta: {
          protocol: {
            name: PROTOCOL,
            logo: "",
            url: "",
          },
          url: "", // TODO:
        },
      };
    })
  );

  return result;
};

export const getUserPositions = async (
  owner: string,
  suiCtx: SuiContext
): Promise<Position> => {
  const result = await Promise.all([
    getUserLPPositions(owner, suiCtx),
    getUserStakingPositions(owner, suiCtx),
  ]);

  return result.flat();
};

const test = async () => {
  if (!process.env.TEST) {
    return;
  }

  const address =
    "0x692853c81afc8f847147c8a8b4368dc894697fc12b929ef3071482d27339815e";
  console.time("getContext");
  const context = await getUserContext(address);
  console.timeEnd("getContext");

  console.time("getUserPositions");
  const result = await getUserPositions(address, context);
  console.log(JSON.stringify(result));
  console.timeEnd("getUserPositions");
};

test();

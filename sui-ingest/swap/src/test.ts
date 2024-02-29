import { SuiEvent } from "./type";
import { processingConfig } from "./main";

const event: SuiEvent = {
  id: {
    txDigest: "5cEjY6chorBjNtgNAZXBm4tvHj9hQo2tfr6zC4fySSXw",
    eventSeq: "0",
  },
  packageId:
    "0x996c4d9480708fb8b92aa7acf819fb0497b5ec8e65ba06601cae2fb6db3312c3",
  transactionModule: "pool_script",
  sender: "0x75aadc57666f6ac6db4fdc7773a83d5648816d7693ae6193623e69fa266b3162",
  type: "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent",
  parsedJson: {
    after_sqrt_price: "4722087991950135434",
    amount_in: "197074323671",
    amount_out: "3000000000010",
    atob: false,
    before_sqrt_price: "4722011755416919193",
    fee_amount: "492685810",
    partner:
      "0xbeef980cf46699aabc35b85cb12f038cc958780808598da177dce1a18d67c096",
    pool: "0x2e041f3fd93646dcc877f783c1f2b7fa62d30271bdef1f21ef002cebf857bded",
    ref_amount: "78829729",
    steps: "1",
    vault_a_amount: "18591790547167258",
    vault_b_amount: "676818188076758",
  },
  timestampMs: "10000",
  dateKey: "123123",
  checkpoint: "27278251",
  gasUsed: {},
};

processingConfig.handler([event]);

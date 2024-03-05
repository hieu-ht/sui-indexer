import { SuiEvent } from "./type";
import { processingConfig } from "./main";

const event: SuiEvent = {
  id: {
    txDigest: "4c372pUMi927jfSDc7UjCPmYJRGMhWmwFPkRgVCK7GYE",
    eventSeq: "1",
  },
  packageId:
    "0x996c4d9480708fb8b92aa7acf819fb0497b5ec8e65ba06601cae2fb6db3312c3",
  transactionModule: "router",
  sender: "0x9dd528ea7b5b3dc4c9ed48a387e1264aa3de2e6e2e7737e7a9190cb0e8d5e13e",
  type: "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent",
  parsedJson: {
    after_sqrt_price: "542792116802834413216",
    amount_in: "3775611",
    amount_out: "3238884246",
    atob: true,
    before_sqrt_price: "543224071962212656123",
    fee_amount: "37757",
    partner:
      "0x0000000000000000000000000000000000000000000000000000000000000000",
    pool: "0x014abe87a6669bec41edcaa95aab35763466acb26a46d551325b07808f0c59c1",
    ref_amount: "0",
    steps: "1",
    vault_a_amount: "961405340",
    vault_b_amount: "1308990295130",
  },
  timestampMs: "10000",
  dateKey: "123123",
  checkpoint: "27949512",
  gasUsed: {},
};

processingConfig.handler([event]);

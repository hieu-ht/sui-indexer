import handles from "./indexer";
// import { getUserPositions } from "./position";

const PROTOCOL = "BlueMove";

const getUserPositions = () => Promise.resolve([]);

export default { indexer: handles, getUserPositions, PROTOCOL };

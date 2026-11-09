import config from "./jest.config";

export default {
    ...config,
    testRegex: "/graph-tests/.*\\.test\\.ts$",
    collectCoverage: false,
};

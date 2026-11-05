import config from "./jest.config";

export default {
    ...config,
    testRegex: "/tests/GraphUnit\\.test\\.ts$",
    setupFilesAfterEnv: [],
    collectCoverage: false,
};

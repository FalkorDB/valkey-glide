import config from "./jest.config";

export default {
    ...config,
    testRegex: "/tests/GraphClientLifecycle\\.test\\.ts$",
    setupFilesAfterEnv: [],
    collectCoverage: false,
};

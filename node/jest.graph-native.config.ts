import config from "./jest.config";

export default {
    ...config,
    testRegex: "/tests/(GraphClientLifecycle|GraphWorkerStack)\\.test\\.ts$",
    setupFilesAfterEnv: [],
    collectCoverage: false,
};

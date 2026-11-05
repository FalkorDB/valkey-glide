/*
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */

import { describe, expect, it } from "@jest/globals";
import { spawnSync } from "node:child_process";

describe("native pinned worker stack contract", () => {
    it.each([64 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024])(
        "handles startup RUST_MIN_STACK=%i without leaking a deferred or provider claim",
        (stack) => {
            const child = spawnSync(
                process.execPath,
                [
                    "-e",
                    String.raw`
const assert = require("node:assert/strict");
const native = require(process.argv[1]);
const { connection_request: proto } = require(process.argv[2]);
const small = Number(process.env.RUST_MIN_STACK) < 2097152;
(async () => {
    const key = small ? native.registerCredentialProvider(() => ({
        accessKeyId: "test-access-key",
        secretAccessKey: "test-secret-key",
    })) : undefined;
    try {
        const request = proto.ConnectionRequest.encode(proto.ConnectionRequest.create({
            addresses: [{ host: "127.0.0.1", port: 1 }],
            lazyConnect: true,
            ...(small ? {
                credentialProviderKey: key,
                tlsMode: proto.TlsMode.SecureTls,
                authenticationInfo: {
                    username: "test-user",
                    iamCredentials: {
                        clusterName: "test-cluster",
                        region: "us-east-1",
                        serviceType: proto.ServiceType.ELASTICACHE,
                    },
                },
            } : {}),
        })).finish();
        for (let attempt = 0; attempt < 2; attempt++) {
            if (small) {
                await assert.rejects(native.CreateDirectClient(request, () => {}), {
                    code: "InvalidArg",
                    message: /RUST_MIN_STACK must be at least 2097152/,
                });
            } else {
                const handle = await native.CreateDirectClient(request, () => {});
                handle.close();
            }
        }
    } finally {
        if (key !== undefined) native.removeCredentialProvider(key);
    }
    process.stdout.write("worker stack contract passed\n");
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
`,
                    require.resolve("../build-ts/native"),
                    require.resolve("../build-ts/ProtobufMessage"),
                ],
                {
                    env: { ...process.env, RUST_MIN_STACK: String(stack) },
                    encoding: "utf8",
                    timeout: 10_000,
                },
            );
            expect({
                error: child.error,
                signal: child.signal,
                status: child.status,
                stderr: child.stderr,
            }).toEqual({
                error: undefined,
                signal: null,
                status: 0,
                stderr: "",
            });
            expect(child.stdout).toContain("worker stack contract passed");
        },
    );
});

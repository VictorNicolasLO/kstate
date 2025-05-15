import { describe, it, expect } from "bun:test";
import { buildSnapshotTopicConfig } from "../packages/kstate/builders";

describe("buildSnapshotTopicConfig", () => {
  it("should return a valid snapshot topic config", () => {
    const config = buildSnapshotTopicConfig("snapshots", {
      topics: [
        {
          name: "test",
          partitions: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 1,
              replicas: [1, 2],
              isr: [1, 2]
            }
          ]
        }
      ]
    });
    expect(config).toBeDefined();
    expect(config.topics[0].topic).toBe("snapshots");
  });
});

---
provenance: authored
owner: org/founder
tags: []
---
# Stelae registry

The registry software that serves stele repositories: **zot over an S3
object store**, packaged so that no cluster, bucket, hostname or credential
is assumed. This directory is the instance-agnostic half. This domain's own
instance — m2 EKS over R2, `oci.stelae.store` — is
[`solution/stelae/ops/`](../../../ops/)
and is operated through the
[`stelae-registry-ops`](../../../skills/registry-ops/SKILL.md) skill.

Why zot: real registry software over an object store pays ~44 ms per request
where the stock `cloudflare/serverless-registry` Worker paid ~3 s per `PATCH`
and capped a whole mainnet publish at 2.40 MB/s
([`kb/registry-alternatives/`](../../../kb/registry-alternatives/), decision
[0034](../../../../../decisions/0034-stelae-registry-front-end.md)). Why a cluster:
the first zot deployment, in a Cloudflare Container behind a Worker, killed
its own write path under sustained upload and could not be probed when it
broke ([`kb/zot-instance-disconnects/`](../../../kb/zot-instance-disconnects/),
decision [0037](../../../../../decisions/0037-stelae-registry-on-eks.md)).

```
publisher ── in-cluster, plain HTTP ──▶ zot ──▶ object store (S3 API)
client ──── public name, TLS ─────────▶ zot ──▶ 307 to a presigned store URL
```

| path | holds |
|---|---|
| `chart/` | the Helm chart: one zot replica, `Recreate`, a PVC for the metaDB, a ClusterIP Service for the write path, and two mutually exclusive public-read shapes (ingress or LoadBalancer), both off by default |
| `zot-build/` | an interim zot image, v2.1.20 plus upstream's unreleased blob-`HEAD` walk fix (#4350); retired at v2.1.21 by [stelae-zot-upstream-repin](../../../../../plans/stelae-zot-upstream-repin.md) |

## The chart

`values.yaml` is deliberately unusable as-is: every instance fact — store
endpoint and bucket, usernames, placement, public hostname — is empty and
`required` in the templates, so a missing value fails at render rather than
deploying a half-configured registry. The values that look arbitrary are
load-bearing, and each carries its reason in the file:

- **`redirectBlobURL: true`.** A blob `GET` answers 307 to a presigned store
  URL, so payload bytes never enter the pod. Proxied reads would meter every
  restore through the cluster and give up the store's egress terms; a blob
  `GET` answering `200` means the deployment is wrong.
- **`rootDirectory: "/"`.** The S3 driver applies it twice — `"/zot"` puts
  keys at `zot/zot/…` and an empty value is rejected — so `"/"` is the only
  value that yields one un-prefixed layout at the bucket root, which is the
  layout the bucket holds. `chunkSize` and `multipartCopyThresholdSize` are
  JSON **strings** for the same kind of reason: as numbers, zot refuses to
  start.
- **No dedupe, no cache driver, no gc.** Dedupe needs a cache driver, which
  for a remote store means a remote dependency, and it is an optimization
  rather than a correctness property. gc is off because every blob is
  referenced by its own `epoch-{E}` tag and nothing is collectable anyway.
- **The metaDB on a PVC.** zot rebuilds its metaDB by parsing every manifest
  in the bucket unless it finds its fast-restart stamp. On ephemeral storage
  that parse ran on every start and grew with the repository, which is what
  crash-looped the registry on its previous substrate; persisted, boot is
  seconds.
- **10-minute HTTP timeouts.** A monolithic layer `PUT` has to stream and
  commit to the store inside this window; at zot's 60 s default, mid-size
  layers behind a busy commit queue were killed and retried forever.

Secrets are referenced, never templated: the htpasswd Secret (bcrypt only —
zot reads nothing else) and the store credentials, whose keys must be named
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` because they reach the S3
driver through the AWS credential chain. The usernames live in both the
values and the htpasswd lines and only convention keeps them matched;
divergence authenticates a user and grants nothing.

Render and install are the instance's business — for this domain, the two
commands in the ops skill. `templates/NOTES.txt` prints the smoke test: a
`401` on `/v2/`, a sub-second blob `HEAD`, and a `307` on a blob `GET`.

## The interim image

zot v2.1.17–v2.1.20 resolve a blob `HEAD`'s Content-Type by walking the
repository's manifests out of the object store — O(tags) per request, 143 s
on `cardano/mainnet` at ~300 tags, which made publishing impossible.
Upstream fixed it in fe0679da (#4350), unreleased as of 2026-08-29, and no
released version both has `redirectBlobURL` and lacks the walk. `zot-build/`
carries exactly that one patch on top of v2.1.20; its Dockerfile holds the
build-and-push commands, and the pushed digest goes into the instance's
values. The acceptance test is the sub-second blob `HEAD`: stock takes 143 s
on the reference digest, patched takes 0.8 s.

## History

Two Cloudflare front ends preceded this: the stock `serverless-registry`
Worker over the `stelae-registry` bucket, and the first zot deployment in a
Cloudflare Container behind a Worker router. Both were decommissioned under
[stelae-registry-decommission](../../../../../plans/stelae-registry-decommission.md).
The attempts that localized the Container fault are recorded in
[`kb/zot-instance-disconnects/`](../../../kb/zot-instance-disconnects/); the deploy
unit itself lives in git history at the 2026-08-29 commit.

The steles themselves were moved once, on 2026-08-28, from the retired
Worker's R2 layout into zot's by a control-plane copy tool that lived here
as `migrate/`. It was removed on 2026-09-06: its source layout no longer
exists and everything it carried over has since been re-published from
scratch, so nothing remained for it to convert or verify. The account of
that job — cost, method, byte-for-byte verification — is
[`kb/registry-alternatives/migration.md`](../../../kb/registry-alternatives/migration.md).

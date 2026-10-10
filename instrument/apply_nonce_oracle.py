#!/usr/bin/env python3
"""Instrument a Fiber checkout with the MuSig2 nonce-use oracle (test builds only).

    instrument/apply_nonce_oracle.py <fiber-checkout>

Adds the `nonce-oracle` cargo feature (and `nonce-oracle-plant`, for the planted-reuse
self-test) to fiber-lib, copies nonce_oracle.rs next to it, and hooks every MuSig2 partial
signing site:

  V1 (crates/fiber-lib/src/fiber/channel.rs)
    Musig2SignContext::sign                 Sent        commitment, revocation and closing
    Musig2SignContext::sign_and_aggregate   Aggregated  local transactions
    channel announcement                    Sent
  V2, when crates/fiber-lib/src/fiber/session_v2.rs exists (#1677)
    guarded_sign                            Sent, or Cached when it returns a stored signature
    bind_context                            Conflict when it refuses a different context

plus a `Reestablish` record when a V1 or V2 channel starts reestablishing, and a `Replay`
record when a V2 reestablishment resends its stored CommitmentSigned or RevokeAndAck
verbatim. The hooks only log; they never change what is signed or sent. Only hashes of the public nonce and of the
signing context are written, never a secret nonce.

Every edit must match exactly once, or the script stops without writing anything. Tested
on develop 2ba4b255 and on #1677 393b77fc.

Build:
    cargo build --release --locked -p fiber-bin --features fnn/nonce-oracle
"""
import pathlib
import re
import shutil
import sys

root = pathlib.Path(sys.argv[1])
module_src = pathlib.Path(__file__).with_name("nonce_oracle.rs")

CFG = '#[cfg(any(test, feature = "nonce-oracle"))]'
TRACK = '#[cfg_attr(any(test, feature = "nonce-oracle"), track_caller)]'
ORACLE = "crate::fiber::nonce_oracle"

edits = {
    "crates/fiber-lib/Cargo.toml": [
        (
            "debug-add-tlc = []\n",
            'debug-add-tlc = []\nnonce-oracle = []\nnonce-oracle-plant = ["nonce-oracle"]\n',
        ),
    ],
    "crates/fiber-lib/src/fiber/mod.rs": [
        ("mod key;\n", f"mod key;\n{CFG}\npub(crate) mod nonce_oracle;\n"),
    ],
    "crates/fiber-lib/src/fiber/channel.rs": [
        # Musig2SignContext::sign: the partial signature is sent to the peer.
        (
            "    fn sign(&self, message: &[u8]) -> Result<PartialSignature, SigningError> {\n",
            f"    {TRACK}\n    fn sign(&self, message: &[u8]) -> Result<PartialSignature, SigningError> {{\n",
        ),
        (
            """        sign_partial(
            &self.common_ctx.key_agg_ctx,
            self.seckey.clone(),
            self.secnonce.clone(),
            &self.common_ctx.agg_nonce,
            message,
        )
    }

    fn sign_and_aggregate(
""",
            f"""        let signature: PartialSignature = sign_partial(
            &self.common_ctx.key_agg_ctx,
            self.seckey.clone(),
            self.secnonce.clone(),
            &self.common_ctx.agg_nonce,
            message,
        )?;
        {CFG}
        {ORACLE}::record(
            {ORACLE}::Use::Sent,
            &self.secnonce,
            &self.common_ctx.key_agg_ctx,
            &self.common_ctx.agg_nonce,
            message,
        );
        Ok(signature)
    }}

    {TRACK}
    fn sign_and_aggregate(
""",
        ),
        # Musig2SignContext::sign_and_aggregate: the partial signature stays local.
        (
            """        let local_signature = sign_partial(
            &self.common_ctx.key_agg_ctx,
            self.seckey.clone(),
            self.secnonce.clone(),
            &self.common_ctx.agg_nonce,
            message,
        )?;
""",
            f"""        let local_signature = sign_partial(
            &self.common_ctx.key_agg_ctx,
            self.seckey.clone(),
            self.secnonce.clone(),
            &self.common_ctx.agg_nonce,
            message,
        )?;
        {CFG}
        {ORACLE}::record(
            {ORACLE}::Use::Aggregated,
            &self.secnonce,
            &self.common_ctx.key_agg_ctx,
            &self.common_ctx.agg_nonce,
            message,
        );
""",
        ),
        # Channel announcement: its nonce is fixed per channel, and the signature is sent.
        (
            """        let partial_signature: PartialSignature = sign_partial(
            &key_agg_ctx,
            &self.signer.funding_key,
            local_secnonce,
""",
            f"""        {CFG}
        {ORACLE}::record(
            {ORACLE}::Use::Sent,
            &local_secnonce,
            &key_agg_ctx,
            &agg_nonce,
            &message,
        );
        let partial_signature: PartialSignature = sign_partial(
            &key_agg_ctx,
            &self.signer.funding_key,
            local_secnonce,
""",
        ),
        # V1 ReestablishChannel handled outside reconnection (coverage; the reconnect arm is
        # hooked below).
        (
            """            FiberChannelMessage::ReestablishChannel(ref reestablish_channel) => {
                let pending_commit_diff = self.store.get_pending_commit_diff(&state.get_id());
""",
            f"""            FiberChannelMessage::ReestablishChannel(ref reestablish_channel) => {{
                {CFG}
                {ORACLE}::reestablish(state.get_id().as_ref(), 1);
                let pending_commit_diff = self.store.get_pending_commit_diff(&state.get_id());
""",
        ),
    ],
}

if (root / "crates/fiber-lib/src/fiber/session_v2.rs").exists():
    edits["crates/fiber-lib/src/fiber/session_v2.rs"] = [
        (
            "pub(super) fn guarded_sign(\n",
            f"""{CFG}
fn oracle_label(record: &SigningNonceV2) -> String {{
    {ORACLE}::v2_label(
        record.channel_id.as_ref(),
        &record.owner.serialize(),
        record.number,
        match record.purpose {{
            NoncePurposeV2::Commitment => "commitment",
            NoncePurposeV2::Revocation => "revocation",
            NoncePurposeV2::Closing => "closing",
        }},
    )
}}

{TRACK}
pub(super) fn guarded_sign(
""",
        ),
        (
            """    if let Some(signature) = record.signature {
        return Ok(signature);
    }
""",
            f"""    if let Some(signature) = record.signature {{
        {CFG}
        {ORACLE}::record_public(
            {ORACLE}::Use::Cached,
            &record.public_nonce,
            aggregate,
            nonce,
            message,
            &oracle_label(record),
        );
        return Ok(signature);
    }}
""",
        ),
        (
            """        .map_err(|error| error.to_string())?;
    record.signature = Some(signature);
""",
            f"""        .map_err(|error| error.to_string())?;
    // A V2 nonce may sign exactly one context, whether the signature is sent or aggregated.
    {CFG}
    {ORACLE}::record_public(
        {ORACLE}::Use::Sent,
        &record.public_nonce,
        aggregate,
        nonce,
        message,
        &oracle_label(record),
    );
    record.signature = Some(signature);
""",
        ),
        (
            "pub(super) fn bind_context(\n",
            f"{TRACK}\npub(super) fn bind_context(\n",
        ),
        (
            """        if previous != &context {
            return Err("Conflicting V2 signing context for consumed nonce".to_owned());
""",
            f"""        if previous != &context {{
            {CFG}
            {ORACLE}::conflict(&record.public_nonce, &oracle_label(record));
            return Err("Conflicting V2 signing context for consumed nonce".to_owned());
""",
        ),
    ]
    edits["crates/fiber-lib/src/fiber/channel_v2.rs"] = [
        (
            """        peer: ReestablishChannelV2,
    ) -> ProcessingChannelResult {
""",
            f"""        peer: ReestablishChannelV2,
    ) -> ProcessingChannelResult {{
        {CFG}
        {ORACLE}::reestablish(state.get_id().as_ref(), 2);
""",
        ),
        (
            """        state.recovery_peer_v2 = Some(peer);
        self.replay_effects_v2(state);
        self.update_tlc_status_on_ack(myself, state).await;
""",
            f"""        {CFG}
        {ORACLE}::replay(state.get_id().as_ref(), replay_cs, ack.is_some());
        state.recovery_peer_v2 = Some(peer);
        self.replay_effects_v2(state);
        self.update_tlc_status_on_ack(myself, state).await;
""",
        ),
    ]

new_texts = {}
for rel, pairs in edits.items():
    path = root / rel
    text = path.read_text()
    for old, new in pairs:
        count = text.count(old)
        if count != 1:
            sys.exit(f"{rel}: expected 1 match, found {count}:\n{old}")
        text = text.replace(old, new)
    new_texts[path] = text

# V1 reestablishment on reconnect: the ReestablishChannel arm taken while the channel is
# reestablishing. Its indentation differs between versions, so match the comment line.
path = root / "crates/fiber-lib/src/fiber/channel.rs"
anchor = re.compile(r"^([ \t]*)// before processing theirs so neither side is left waiting\.\n", re.M)
matches = anchor.findall(new_texts[path])
if len(matches) != 1:
    sys.exit(f"channel.rs: expected 1 reestablish-on-reconnect arm, found {len(matches)}")
new_texts[path] = anchor.sub(
    lambda m: f"{m.group(0)}{m.group(1)}{CFG}\n{m.group(1)}{ORACLE}::reestablish(state.get_id().as_ref(), 1);\n",
    new_texts[path],
)

target = root / "crates/fiber-lib/src/fiber/nonce_oracle.rs"
if target.exists():
    sys.exit(f"{target} already exists: is this checkout instrumented already?")
for path, text in new_texts.items():
    path.write_text(text)
shutil.copyfile(module_src, target)
print("instrumented:", ", ".join(sorted(edits)))

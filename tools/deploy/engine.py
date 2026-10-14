"""Image-only deployment contracts; Python 3.8, no credentials in public results."""
import copy
import hashlib
import json
import re


class GuardError(Exception):
    pass


def need(condition, code):
    if not condition:
        raise GuardError(code)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")


def release_descriptor(value):
    need(isinstance(value, dict), "release_descriptor_required")
    keys = ("version", "sourceSha256", "imageId", "imageTag")
    need(all(isinstance(value.get(key), str) for key in keys), "release_descriptor_invalid")
    need(re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?", value["version"]), "release_version_invalid")
    need(re.fullmatch(r"[0-9a-f]{64}", value["sourceSha256"]), "release_source_invalid")
    need(re.fullmatch(r"sha256:[0-9a-f]{64}", value["imageId"]), "release_image_invalid")
    need(re.fullmatch(r"reolink-bridge:[A-Za-z0-9_.-]{1,100}", value["imageTag"]), "release_tag_invalid")
    need(value.get("testsPassed") is True and value.get("testedApplicationDllIdentical") is True, "tested_release_required")
    result = {key: value[key] for key in keys}
    for key in ("sourceRevision", "archiveSha256", "applicationDllSha256"):
        if key in value:
            size = 40 if key == "sourceRevision" else 64
            need(isinstance(value[key], str) and re.fullmatch(r"[0-9a-f]{%d}" % size, value[key]), "release_provenance_invalid")
            result[key] = value[key]
    return result


def projected(entries):
    keys = ("ip", "id", "name", "imageId", "imageRef", "configSha256", "composeSha256", "mac", "network", "project", "service")
    return [{key: entry[key] for key in keys} for entry in sorted(entries, key=lambda row: row["ip"])]


def target(entries, ip):
    matches = [entry for entry in entries if entry["ip"] == ip]
    need(len(matches) == 1, "exact_target_required")
    return matches[0]


def host_config(entry):
    result = copy.deepcopy(entry["inspect"]["HostConfig"])
    # Array order is immaterial for complete distinct mounts. No member field,
    # duplicate or permission is removed by this normalization.
    mounts = result.get("Mounts")
    if isinstance(mounts, list):
        need(all(isinstance(mount, dict) and isinstance(mount.get("Target"), str) for mount in mounts), "mount_member_invalid")
        need(len({mount["Target"] for mount in mounts}) == len(mounts), "duplicate_mount_target")
        result["Mounts"] = sorted(mounts, key=lambda mount: json.dumps(mount, sort_keys=True, separators=(",", ":")))
    for key in ("Binds", "Mounts", "Dns", "DnsOptions", "DnsSearch"):
        if result.get(key) in (None, []):
            result[key] = []
    return result


def runtime(entry):
    result = copy.deepcopy(entry["inspect"]["Config"])
    result.pop("Image", None)
    if result.get("Hostname") == entry["id"][:12]:
        result.pop("Hostname")
    labels = result.setdefault("Labels", {})
    for key in ("com.docker.compose.image", "com.docker.compose.config-hash", "com.docker.compose.replace"):
        value = labels.pop(key, None)
        need(value is None or isinstance(value, str) and re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", value), "generated_label_invalid")
    return result


def candidate_compose(before, entry, image_tag):
    value = copy.deepcopy(before)
    need(value["services"][entry["service"]]["image"] == entry["imageRef"], "compose_image_drift")
    value["services"][entry["service"]]["image"] = image_tag
    return value


def preserve(before, after, ip, compose_bytes, image_ref, image_id, image_lookup):
    need({entry["ip"] for entry in before} == {entry["ip"] for entry in after} and len(before) == len(after), "inventory_scope_drift")
    for current in after:
        previous = target(before, current["ip"])
        selected = current["ip"] == ip
        for key in ("name", "mac", "network", "project", "service", "directory", "compose", "config", "configSha256", "configuration"):
            need(current[key] == previous[key], "preserved_" + key + "_drift")
        need(current["imageId"] == (image_id if selected else previous["imageId"]) and current["imageRef"] == (image_ref if selected else previous["imageRef"]), "image_drift")
        for filename, value in previous["composeSha256"].items():
            expected = digest(compose_bytes) if filename == previous["compose"][0] else value
            need(current["composeSha256"].get(filename) == expected, "compose_hash_drift")
        need(set(current["composeSha256"]) == set(previous["composeSha256"]), "compose_file_set_drift")
        need(host_config(previous) == host_config(current), "host_settings_drift")
        if not selected:
            need(current["id"] == previous["id"], "unrelated_container_id_drift")
            need(current["inspect"].get("RestartCount") == previous["inspect"].get("RestartCount"), "unrelated_lifecycle_drift")
            for key in ("StartedAt", "RestartCount"):
                need(current["inspect"]["State"].get(key) == previous["inspect"]["State"].get(key), "unrelated_lifecycle_drift")
            need(runtime(previous) == runtime(current), "unrelated_runtime_drift")
        else:
            old_runtime, new_runtime = runtime(previous), runtime(current)
            labels = old_runtime["Labels"]
            explicit = previous["resolved"]["services"][previous["service"]].get("labels", {})
            for key, value in (image_lookup(previous["imageId"])["Config"].get("Labels") or {}).items():
                if key not in explicit and labels.get(key) == value:
                    labels.pop(key)
            for key, value in (image_lookup(image_id)["Config"].get("Labels") or {}).items():
                if key not in explicit:
                    labels[key] = value
            need(old_runtime == new_runtime, "runtime_defaults_drift")


def stable(expected, current, compose_bytes, image_lookup):
    need(projected(expected) == projected(current), "fresh_inventory_required")
    for entry in expected:
        preserve(expected, current, entry["ip"], compose_bytes, entry["imageRef"], entry["imageId"], image_lookup)


def rates(first, second, profiles, reference=None):
    span = second["at"] - first["at"]
    need(span >= 1, "frame_window_too_short")
    need(len(first["streams"]) == len(second["streams"]) == len(profiles), "frame_profile_count_changed")
    result = []
    for index, (left, right) in enumerate(zip(first["streams"], second["streams"])):
        need(left["profile"] == right["profile"], "frame_profile_order_changed")
        delta = right["totalAccessUnits"] - left["totalAccessUnits"]
        floor = max(1, reference[index]["fps"] * .75) if reference else 1
        need(delta >= 0 and delta / span >= floor, "actual_frame_rate_too_low")
        result.append({"profile": right["profile"], "fps": round(delta / span, 3)})
    return result


class DeployEngine:
    """The backend owns filesystem, Docker and time; all public returns are projected."""
    def __init__(self, backend):
        self.backend = backend
        self.ips = tuple(backend.ips)

    def release(self, descriptor):
        frozen = release_descriptor(descriptor)
        image = self.backend.image(frozen["imageId"])
        need(image["Id"] == frozen["imageId"], "immutable_image_mismatch")
        need(self.backend.image(frozen["imageTag"])["Id"] == frozen["imageId"], "release_tag_image_mismatch")
        labels = image["Config"].get("Labels") or {}
        need(labels.get("org.opencontainers.image.revision") == "sha256-" + frozen["sourceSha256"], "release_source_label_mismatch")
        need(labels.get("org.opencontainers.image.version") == frozen["version"], "release_version_label_mismatch")
        return frozen

    def inventory(self):
        entries = self.backend.inventory()
        need(len(entries) == len(self.ips) and {entry["ip"] for entry in entries} == set(self.ips), "exact_inventory_scope_required")
        return entries

    def checked_state(self, baseline, descriptor):
        state = self.backend.read_json(baseline, "state.private.json")
        need(state["release"] == self.release(descriptor), "baseline_release_mismatch")
        need(state["ips"] == list(self.ips), "baseline_scope_mismatch")
        return state

    def plan(self, descriptor):
        frozen = self.release(descriptor)
        entries = self.inventory()
        compose = self.backend.read_compose()
        production = self.backend.image(frozen["imageId"])["Config"]
        for entry in entries:
            need(entry["imageRef"] != frozen["imageTag"] or entry["imageId"] == frozen["imageId"], "release_tag_reuses_running_image_reference")
            need(self.backend.image(entry["imageRef"])["Id"] == entry["imageId"], "original_image_tag_drift")
            old = self.backend.image(entry["imageId"])["Config"]
            for key in ("Env", "User", "Entrypoint", "Cmd", "WorkingDir", "Healthcheck", "ExposedPorts", "Volumes", "StopSignal"):
                need(old.get(key) == production.get(key), "image_default_drift_" + key)
            self.backend.sample(entry)
        baseline = self.backend.create_receipt("plan")
        self.backend.write_private(baseline, "initial.private.json", encoded(entries))
        self.backend.write_private(baseline, "compose-initial.private.json", compose)
        for entry in entries:
            self.backend.write_private(baseline, "config-" + entry["ip"] + ".private.json", self.backend.read_config(entry))
        state = {"schemaVersion": 1, "release": frozen, "ips": list(self.ips), "entries": entries, "receipts": [], "attempts": [], "recoveries": [], "rollbackSteps": [], "rollbackReceipts": []}
        self.backend.write_private(baseline, "state.private.json", encoded(state))
        return {"ok": True, "mode": "plan", "baseline": baseline, "release": frozen, "bridges": projected(entries), "pendingIps": list(self.ips), "credentialsPrinted": False}

    def checkpoint(self, baseline, prior_state, entries, receipt, success):
        state = copy.deepcopy(prior_state)
        state["entries"] = entries
        state["attempts"].append(receipt)
        if success:
            state["receipts"].append(receipt)
        self.backend.replace_private(baseline, "state.private.json", encoded(state), encoded(prior_state))
        return state

    def wait(self, before, entry, compose, image_ref, image_id, reference):
        deadline = self.backend.monotonic() + 150
        last_error = "not_ready"
        while self.backend.monotonic() < deadline:
            try:
                current = self.inventory()
                preserve(before, current, entry["ip"], compose, image_ref, image_id, self.backend.image)
                selected = target(current, entry["ip"])
                first = self.backend.sample(selected)
                self.backend.sleep(5)
                current = self.inventory()
                preserve(before, current, entry["ip"], compose, image_ref, image_id, self.backend.image)
                second = self.backend.sample(target(current, entry["ip"]))
                measured = rates(first, second, entry["configuration"]["onvif"]["profiles"], reference)
                for other in current:
                    self.backend.sample(other)
                return current, {"allHealthy": True, "rates": measured, "sourceMediaVerified": True}
            except GuardError as error:
                last_error = str(error)
                self.backend.sleep(2)
        raise GuardError("readiness_timeout_" + last_error)

    def apply(self, payload):
        frozen = self.release(payload["release"])
        baseline = payload["baseline"]
        state = self.checked_state(baseline, payload["release"])
        need(not state.get("rollbackReceipts"), "rollback_has_started_new_plan_required")
        ip = payload["ip"]
        need(ip == self.ips[len(state["receipts"])] if len(state["receipts"]) < len(self.ips) else False, "ordered_next_target_required")
        before = self.inventory()
        need(payload["expected"] == projected(before), "fresh_inventory_required")
        compose_old = self.backend.read_compose()
        stable(state["entries"], before, compose_old, self.backend.image)
        entry = target(before, ip)
        first = self.backend.sample(entry)
        self.backend.sleep(5)
        second = self.backend.sample(entry)
        reference = rates(first, second, entry["configuration"]["onvif"]["profiles"])
        original = self.backend.read_config(entry)
        compose_new = encoded(candidate_compose(json.loads(compose_old), entry, frozen["imageTag"]))
        receipt = self.backend.create_receipt("apply")
        for name, data in (("before.private.json", encoded(before)), ("config-before.private.json", original),
                           ("compose-before.private.json", compose_old), ("compose-after.private.json", compose_new)):
            self.backend.write_private(receipt, name, data)
        saved = {"schemaVersion": 1, "ip": ip, "baseline": baseline, "release": frozen, "referenceRates": reference}
        self.backend.write_private(receipt, "receipt.private.json", encoded(saved))
        with self.backend.maintenance():
            need(self.backend.read_compose() == compose_old and self.backend.read_config(entry) == original, "files_drift_before_write")
            stable(before, self.inventory(), compose_old, self.backend.image)
            self.release(payload["release"])
            changed = False
            try:
                self.backend.write_compose(compose_new, compose_old)
                changed = True
                resolved = self.backend.resolved(entry)
                expected_resolved = candidate_compose(entry["resolved"], entry, frozen["imageTag"])
                need(resolved == expected_resolved, "resolved_compose_drift")
                self.backend.recreate(entry)
                after, proof = self.wait(before, entry, compose_new, frozen["imageTag"], frozen["imageId"], reference)
                result = {"ok": True, "mode": "apply", "ip": ip, "receipt": receipt, "verification": proof, "otherBridgesPreserved": True, "bridges": projected(after), "credentialsPrinted": False}
                self.backend.write_private(receipt, "result.json", encoded(result))
                self.backend.write_private(receipt, "after.private.json", encoded(after))
                self.checkpoint(baseline, state, after, receipt, True)
                return result
            except Exception as error:
                failure = str(error) if isinstance(error, GuardError) else "deployment_operation_failed"
                try:
                    need(self.backend.read_config(entry) == original, "rollback_config_concurrent_drift")
                    current_compose = self.backend.read_compose()
                    need(current_compose in (compose_old, compose_new), "rollback_compose_concurrent_drift")
                    need(self.backend.image(entry["imageRef"])["Id"] == entry["imageId"], "rollback_image_tag_drift")
                    if current_compose != compose_old:
                        self.backend.write_compose(compose_old, current_compose)
                    if changed:
                        self.backend.recreate(entry)
                    restored, proof = self.wait(before, entry, compose_old, entry["imageRef"], entry["imageId"], reference)
                    result = {"ok": False, "mode": "apply", "ip": ip, "receipt": receipt, "failure": failure, "rollbackRestored": True, "verification": proof, "bridges": projected(restored), "credentialsPrinted": False}
                    self.backend.write_private(receipt, "result.json", encoded(result))
                    self.backend.write_private(receipt, "after.private.json", encoded(restored))
                    self.checkpoint(baseline, state, restored, receipt, False)
                except Exception as recovery:
                    result = {"ok": False, "mode": "apply", "ip": ip, "receipt": receipt, "failure": failure, "rollbackRestored": False,
                              "rollbackFailure": str(recovery) if isinstance(recovery, GuardError) else "recovery_operation_failed", "credentialsPrinted": False}
                    self.backend.write_private(receipt, "failed-result.json", encoded(result))
                return result

    def resume(self, payload):
        state = self.checked_state(payload["baseline"], payload["release"])
        current = self.inventory()
        stable(state["entries"], current, self.backend.read_compose(), self.backend.image)
        for entry in current:
            self.backend.sample(entry)
        rollback_receipts = state.get("rollbackReceipts") or []
        rolling_back = bool(rollback_receipts) and len(state["rollbackSteps"]) < len(rollback_receipts)
        original = self.backend.read_json(payload["baseline"], "initial.private.json")
        restored = not state["receipts"] and all(entry["imageId"] == target(original, entry["ip"])["imageId"] and entry["configSha256"] == target(original, entry["ip"])["configSha256"] for entry in current)
        return {"ok": True, "mode": "resume", "baseline": payload["baseline"], "receipts": state["receipts"], "attempts": state["attempts"],
                "pendingIps": [] if rollback_receipts else list(self.ips[len(state["receipts"]):]), "phase": "rollback-pending" if rolling_back else "rolled-back" if rollback_receipts else "deploying",
                "rollbackSteps": state["rollbackSteps"], "allPreviousImagesRestored": restored, "bridges": projected(current), "credentialsPrinted": False}

    def steps(self, state, require_complete=True):
        need(len(state["receipts"]) == len(self.ips) if require_complete else 0 <= len(state["receipts"]) <= len(self.ips), "complete_ordered_receipts_required")
        result = []
        for index, path in enumerate(state["receipts"]):
            receipt = self.backend.read_json(path, "receipt.private.json")
            proof = self.backend.read_json(path, "result.json")
            need(receipt["ip"] == self.ips[index] and receipt["release"] == state["release"] and proof.get("ok") is True, "successful_ordered_receipt_required")
            before = self.backend.read_json(path, "before.private.json")
            entry = target(before, receipt["ip"])
            old = self.backend.read_private(path, "compose-before.private.json")
            new = self.backend.read_private(path, "compose-after.private.json")
            need(json.loads(new) == candidate_compose(json.loads(old), entry, state["release"]["imageTag"]), "receipt_exceeded_image_change")
            need(digest(self.backend.read_private(path, "config-before.private.json")) == entry["configSha256"], "receipt_config_hash_mismatch")
            need(self.backend.image(entry["imageId"])["Id"] == entry["imageId"], "rollback_image_missing")
            if result:
                need(result[-1]["new"] == old, "receipt_compose_chain_broken")
            result.append({"path": path, "receipt": receipt, "before": before, "entry": entry, "old": old, "new": new})
        return result

    def final(self, payload):
        state = self.checked_state(payload["baseline"], payload["release"])
        need(not state["rollbackSteps"], "rollback_in_progress")
        steps = self.steps(state)
        if "receipts" in payload:
            need(payload["receipts"] == state["receipts"], "receipt_list_mismatch")
        current = self.inventory()
        need(payload["expected"] == projected(current), "fresh_inventory_required")
        compose = self.backend.read_compose()
        need(compose == steps[-1]["new"], "final_compose_chain_drift")
        stable(state["entries"], current, compose, self.backend.image)
        original = self.backend.read_json(payload["baseline"], "initial.private.json")
        for entry in current:
            prior = target(original, entry["ip"])
            need(entry["configuration"] == prior["configuration"] and entry["configSha256"] == prior["configSha256"], "final_configuration_changed")
            need(entry["imageId"] == state["release"]["imageId"], "final_release_image_mismatch")
            self.backend.sample(entry)
        result = {"ok": True, "mode": "final", "baseline": payload["baseline"], "rollbackManifest": payload["baseline"], "receipts": state["receipts"], "release": state["release"],
                  "allHealthy": True, "exactConfigurationsPreserved": True, "finiteRollbackSteps": len(steps), "bridges": projected(current), "credentialsPrinted": False}
        self.backend.write_private(payload["baseline"], "final-result.json", encoded(result))
        return result

    def rollback(self, payload):
        """Undo the last accepted NAS step after an external media gate fails."""
        baseline = payload["baseline"]
        state = self.checked_state(baseline, payload["release"])
        need(not state["rollbackSteps"] and state["receipts"] and payload["receipt"] == state["receipts"][-1], "last_successful_receipt_required")
        receipt = payload["receipt"]
        saved = self.backend.read_json(receipt, "receipt.private.json")
        need(saved["baseline"] == baseline and saved["release"] == state["release"], "rollback_receipt_release_mismatch")
        before = self.backend.read_json(receipt, "before.private.json")
        original = target(before, saved["ip"])
        current = self.inventory()
        need(payload["expected"] == projected(current), "fresh_inventory_required")
        compose = self.backend.read_compose()
        stable(state["entries"], current, compose, self.backend.image)
        need(compose == self.backend.read_private(receipt, "compose-after.private.json"), "rollback_compose_chain_drift")
        selected = target(current, saved["ip"])
        need(self.backend.read_config(selected) == self.backend.read_private(receipt, "config-before.private.json"), "rollback_configuration_drift")
        need(self.backend.image(original["imageRef"])["Id"] == original["imageId"], "rollback_image_tag_drift")
        old_compose = self.backend.read_private(receipt, "compose-before.private.json")
        with self.backend.maintenance():
            stable(state["entries"], self.inventory(), compose, self.backend.image)
            self.backend.write_compose(old_compose, compose)
            self.backend.recreate(selected)
            restored, proof = self.wait(current, selected, old_compose, original["imageRef"], original["imageId"], saved["referenceRates"])
            updated = copy.deepcopy(state)
            updated["entries"] = restored
            updated["receipts"].pop()
            updated.setdefault("recoveries", []).append(receipt)
            self.backend.replace_private(baseline, "state.private.json", encoded(updated), encoded(state))
            result = {"ok": True, "mode": "rollback", "receipt": receipt, "baseline": baseline, "ip": saved["ip"], "rollbackRestored": True,
                      "verification": proof, "bridges": projected(restored), "originalReceiptRetained": True, "credentialsPrinted": False}
            self.backend.write_private(receipt, "rollback-result.json", encoded(result))
            return result

    def rollback_all(self, payload):
        baseline = payload["baseline"]
        state = self.checked_state(baseline, payload["release"])
        current = self.inventory()
        need(payload["expected"] == projected(current), "fresh_inventory_required")
        compose = self.backend.read_compose()
        stable(state["entries"], current, compose, self.backend.image)
        with self.backend.maintenance():
            if not state.get("rollbackReceipts"):
                updated = copy.deepcopy(state)
                updated["rollbackReceipts"] = list(state["receipts"])
                self.backend.replace_private(baseline, "state.private.json", encoded(updated), encoded(state))
                state = updated
            chain = copy.deepcopy(state)
            chain["receipts"] = state["rollbackReceipts"]
            steps = self.steps(chain, require_complete=False)
            for step in list(reversed(steps))[len(state["rollbackSteps"]):]:
                current = self.inventory()
                stable(state["entries"], current, compose, self.backend.image)
                need(compose == step["new"], "rollback_compose_chain_drift")
                entry = target(current, step["entry"]["ip"])
                need(self.backend.read_config(entry) == self.backend.read_private(step["path"], "config-before.private.json"), "rollback_configuration_drift")
                original = step["entry"]
                need(self.backend.image(original["imageRef"])["Id"] == original["imageId"], "rollback_image_tag_drift")
                self.backend.write_compose(step["old"], compose)
                self.backend.recreate(entry)
                restored, proof = self.wait(current, entry, step["old"], original["imageRef"], original["imageId"], step["receipt"]["referenceRates"])
                new_state = copy.deepcopy(state)
                new_state["entries"] = restored
                need(new_state["receipts"] and new_state["receipts"][-1] == step["path"], "rollback_active_chain_mismatch")
                new_state["receipts"].pop()
                new_state["rollbackSteps"].append({"ip": entry["ip"], "verification": proof})
                self.backend.replace_private(baseline, "state.private.json", encoded(new_state), encoded(state))
                state, compose = new_state, step["old"]
        initial = self.backend.read_json(baseline, "initial.private.json")
        for entry in state["entries"]:
            original = target(initial, entry["ip"])
            need(entry["imageId"] == original["imageId"] and entry["configSha256"] == original["configSha256"], "whole_rollback_original_state_mismatch")
        need(compose == self.backend.read_private(baseline, "compose-initial.private.json"), "whole_rollback_original_compose_mismatch")
        return {"ok": True, "mode": "rollback-all", "baseline": baseline, "steps": state["rollbackSteps"], "receipts": state["receipts"], "allPreviousImagesRestored": True,
                "bridges": projected(state["entries"]), "configurationBytesPreserved": True, "credentialsPrinted": False}

    def run(self, payload):
        mode = payload.get("mode")
        if mode == "inventory":
            return {"ok": True, "mode": mode, "bridges": projected(self.inventory()), "credentialsPrinted": False}
        if mode == "plan":
            return self.plan(payload["release"])
        operations = {"apply": self.apply, "resume": self.resume, "final": self.final, "rollback": self.rollback, "rollback-all": self.rollback_all}
        need(mode in operations, "unsupported_deployment_mode")
        return operations[mode](payload)

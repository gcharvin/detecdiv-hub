from worker.path_mappings import WorkerPathMapping, map_payload_path_fields


def test_external_classifier_package_links_follow_worker_storage_mapping():
    mappings = (WorkerPathMapping("/data", "//host/DATA"),)
    pipeline = {"nodes": [{"customPackageRoot": "/data/code/classifier", "customPackageDir": "/data/code/classifier/+custom", "pkg": "custom"}]}
    mapped = map_payload_path_fields(pipeline, mappings)
    assert mapped["nodes"][0]["customPackageRoot"] == r"\\host\DATA\code\classifier"
    assert mapped["nodes"][0]["customPackageDir"] == r"\\host\DATA\code\classifier\+custom"
    assert mapped["nodes"][0]["pkg"] == "custom"

from pathlib import Path

path = Path("src/server.rs")
text = path.read_text()
old = '''                #[cfg(feature = "cluster")]
                if cluster_enabled && let Some(mgr) = rtmp_bridge.cluster_manager() {
                    for frame in exported_frames {
                        let sid = rtmp_bridge.stream_id_for_conn(frame.publisher_conn_id);
'''
new = '''                #[cfg(feature = "cluster")]
                if cluster_enabled && let Some(mgr) = rtmp_bridge.cluster_manager() {
                    for frame in exported_frames {
                        let generation = publisher_generation(frame.publisher_conn_id);
                        if publish_generations_before_poll
                            .get(&frame.publisher_conn_id)
                            .is_some_and(|before| *before != generation)
                        {
                            // A republish during this poll makes the buffered frame batch
                            // ambiguous for cluster export as well. Do not stamp old frames
                            // with the new stream/ownership epoch.
                            continue;
                        }
                        let sid = rtmp_bridge.stream_id_for_conn(frame.publisher_conn_id);
'''
if text.count(old) != 1:
    raise SystemExit(f"expected cluster export block once, found {text.count(old)}")
path.write_text(text.replace(old, new, 1))

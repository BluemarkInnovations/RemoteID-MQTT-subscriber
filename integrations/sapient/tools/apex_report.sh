#!/bin/sh
# Summary of what the Apex test middleware (integrations/sapient/docker) received:
# messages per type, how many it forwarded, and every validation error.
# Apex drops invalid messages silently; their errors are only in its SQLite archive.
#     integrations/sapient/tools/apex_report.sh [minutes, default 60]
MINUTES=${1:-60}
cd "$(dirname "$0")/../docker" || exit 1
docker compose exec -T apex python - "$MINUTES" <<'PY'
import glob, sqlite3, sys
import time
files = sorted(glob.glob('/apex/data/**/*.sqlite', recursive=True))
if not files:
    sys.exit('no Apex database yet')
since = int((time.time() - 60 * int(sys.argv[1])) * 1e6)      # Apex stores epoch microseconds
db = sqlite3.connect(files[-1])
print('messages from edge nodes in the last %s minutes (%s):' % (sys.argv[1], files[-1].rsplit('/', 1)[-1]))
rows = db.execute("""select m.parsed_type, count(*), sum(m.forwarded_count > 0), sum(m.error_severity is not null)
                     from Message m join Connection c on c.id = m.connection_id
                     where c.client_type = 'Child' and m.timestamp_received >= ? group by m.parsed_type""", (since,))
print('    %-20s %8s %10s %8s' % ('type', 'received', 'forwarded', 'errors'))
for t, n, fwd, err in rows:
    print('    %-20s %8d %10d %8d' % (t or '(not decoded)', n, fwd or 0, err or 0))
errors = db.execute("""select m.parsed_type, m.parsed_node_id, m.error_severity, m.error_description, count(*)
                       from Message m where m.error_severity is not null and m.timestamp_received >= ?
                       group by 1, 2, 3, 4 order by 5 desc limit 50""", (since,)).fetchall()
print('validation errors:' if errors else 'no validation errors')
for t, node, sev, desc, n in errors:
    print('    %4dx %s from %s (%s): %s' % (n, t or 'message', node or '?', sev,
                                         desc.replace(chr(10), chr(10) + ' ' * 10)))
PY

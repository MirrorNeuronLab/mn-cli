"""LAN-only Syncthing startup under the actual shared-filesystem owner."""

SHARED_STORAGE_OWNER_SCRIPT = """shared_dir="${MN_SYNCTHING_FOLDER_PATH:-/var/syncthing/MirrorNeuronShared}"
# Read ownership inside Docker, where bind-mount/user-namespace IDs are authoritative.
shared_owner="$(stat -c '%u:%g' "$shared_dir")"
PUID="${shared_owner%:*}"
PGID="${shared_owner#*:}"
export PUID PGID
"""

SYNCTHING_LAN_ONLY_ENTRYPOINT_SCRIPT = "set -eu\n" + SHARED_STORAGE_OWNER_SCRIPT + """config_dir="${STHOMEDIR:-/var/syncthing/config}"
config_file="$config_dir/config.xml"
mkdir -p "$config_dir"

if [ ! -f "$config_file" ]; then
  /bin/syncthing generate --home "$config_dir" --no-port-probing
fi

sed -i \\
  -e 's#<relaysEnabled>[^<]*</relaysEnabled>#<relaysEnabled>false</relaysEnabled>#' \\
  -e 's#<globalAnnounceEnabled>[^<]*</globalAnnounceEnabled>#<globalAnnounceEnabled>false</globalAnnounceEnabled>#' \\
  -e 's#<natEnabled>[^<]*</natEnabled>#<natEnabled>false</natEnabled>#' \\
  -e 's#<localAnnounceEnabled>[^<]*</localAnnounceEnabled>#<localAnnounceEnabled>true</localAnnounceEnabled>#' \\
  -e 's#<stunKeepaliveStartS>[^<]*</stunKeepaliveStartS>#<stunKeepaliveStartS>0</stunKeepaliveStartS>#' \\
  -e 's#<urAccepted>[^<]*</urAccepted>#<urAccepted>-1</urAccepted>#' \\
  -e 's#<autoUpgradeIntervalH>[^<]*</autoUpgradeIntervalH>#<autoUpgradeIntervalH>0</autoUpgradeIntervalH>#' \\
  -e 's#<crashReportingEnabled>[^<]*</crashReportingEnabled>#<crashReportingEnabled>false</crashReportingEnabled>#' \\
  "$config_file"

# The Syncthing image starts as root for setup, then drops to PUID:PGID.  Its
# generated config files otherwise remain root-owned and make the sidecar
# crash-loop on its next start when Syncthing needs to update its certificates.
chown -R "$PUID:$PGID" "$config_dir"

exec /bin/entrypoint.sh /bin/syncthing serve"""

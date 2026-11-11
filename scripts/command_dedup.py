import re
import shlex


RESOURCE_OPTIONS = frozenset((
    "--stack-name", "--stack-set-name", "--cluster-name", "--name", "--namespace", "-n",
    "--resource-group", "--account-name", "--container-name", "--bucket", "--source",
    "--db-instance-identifier", "--db-cluster-identifier", "--db-snapshot-identifier",
    "--db-cluster-snapshot-identifier", "--snapshot-name", "--replication-group-id",
    "--cache-cluster-id", "--cluster-identifier", "--database-name", "--table-name",
    "--stack", "--project", "--region", "--zone", "--context", "--kube-context",
))
ENVIRONMENT = re.compile(r"(?i)(?:^|[-_./])((?:production|prod|staging|stage|development|dev|test))(?:$|[-_./])")
POSITIONAL_RESOURCE = {
    ("kubectl", "delete", "namespace"), ("kubectl", "delete", "ns"),
    ("helm", "uninstall"), ("helm", "delete"),
    ("oc", "delete", "project"), ("dropdb",),
}


def resource_shape(value):
    markers = tuple(sorted({match.lower() for match in ENVIRONMENT.findall(value)}))
    if any(character in value for character in "$`*?[]{}"):
        return value
    return "<resource:" + ",".join(markers) + ">"


def near_key(candidate):
    row = candidate["row"]
    try:
        tokens = shlex.split(row["command"].replace("\\\n", " "), posix=True)
    except ValueError:
        return None
    normalized = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        option, separator, value = token.partition("=")
        if option in RESOURCE_OPTIONS:
            if separator:
                normalized.extend((option, resource_shape(value)))
            elif index + 1 < len(tokens):
                normalized.extend((option, resource_shape(tokens[index + 1])))
                index += 1
            else:
                normalized.append(token)
        elif token.startswith(("s3://", "gs://", "b2://")):
            scheme, path = token.split("://", 1)
            parts = path.split("/")
            normalized.append(scheme + "://" + "/".join(resource_shape(part) if part else "" for part in parts))
        elif any(tuple(tokens[:len(prefix)]) == prefix and index == len(prefix)
                 for prefix in POSITIONAL_RESOURCE):
            normalized.append(resource_shape(token))
        else:
            normalized.append(token)
        index += 1
    return (row["risk"], row["class"], row["severity"], candidate["role"], tuple(normalized))

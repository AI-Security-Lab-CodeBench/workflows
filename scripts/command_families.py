from typing import Final


VALUE_FLAGS: Final = frozenset(
    "--profile --region --project --zone --configuration --context --kube-context "
    "--kubeconfig --namespace -n --chdir -chdir --host -H --config --user -u "
    "--directory --stack -s --app --output -o --file -f --dbname -d --database "
    "--command -c --execute -e --filter --name --resource-group --account-name "
    "--container-name --auth-mode --stack-name --stack-set-name --stage --min-size".split()
)

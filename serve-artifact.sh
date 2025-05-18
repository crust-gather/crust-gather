#!/usr/bin/env -S nix shell nixpkgs#bash nixpkgs#curl nixpkgs#unzip nixpkgs#mktemp github:crust-gather/crust-gather --command bash


usage() {
    cat << EOF
Usage: $0 -o OWNER -r REPO -a ARTIFACT_ID [-u URI] -s SOCKET

Script to collect a Github Actions artifact with crust-gather content and serve it using kubeconfig.

-u, --uri         Full artifact URL for download. Will be parsed to owner/repo/artifact string. Required if -u is not specified.
-o, --owner       Owner of the repository. Required.
-r, --repo        Repository name. Required.
-a, --artifact_id Artifact ID. Required.
-s, --socket      The socket address to bind the HTTP server to. Defaults to 0.0.0.0:9095
EOF
    exit 1
}

while [[ $# -gt 0 ]]
do
    key="$1"
    case "${key}" in
        -u|--uri)
            URI="${2}"
            shift
            ;;
        -o|--owner)
            OWNER="${2}"
            shift
            ;;
        -r|--repo)
            REPO="${2}"
            shift
            ;;
        -a|--artifact_id)
            ARTIFACT_ID="${2}"
            shift
            ;;
        -s|--socket)
            SOCKET="-s ${2}"
            shift
            ;;
        *)
            # unknown option
            usage
            ;;
    esac
    shift
done

if [[ -z ${OWNER} ]] || [[ -z ${REPO} ]] || [[ -z ${ARTIFACT_ID} ]] && [[ -z ${URI} ]] then
    usage
    exit
fi

if [[ -n ${URI} ]] then
    # Extract owner and repo using regex
    OWNER=$(echo "$URI" | cut -d\/ -f4)
    REPO=$(echo "$URI" | cut -d\/ -f5)

    # Extract artifact ID
    ARTIFACT_ID=${URI##*/}

    # Split the artifact ID to get the actual ID (everything after the last '/')
    ARTIFACT_ID=${ARTIFACT_ID##*/}
    ARTIFACT_ID=${ARTIFACT_ID%%/*}
fi

artifact_download="https://api.github.com/repos/${OWNER}/${REPO}/actions/artifacts/${ARTIFACT_ID}/zip"
echo "Downloading from $artifact_download"

tmp=$(mktemp -d)
curl -H "Accept: application/vnd.github+json" -H "Authorization: token ${GITHUB_TOKEN}" -L  ${artifact_download} --output ${tmp}/artifact.zip
unzip ${tmp}/artifact.zip -d ${tmp}

echo "Serving on ${SOCKET:-0.0.0.0:9095}..."
kubectl-crust-gather serve -a ${tmp} ${SOCKET}

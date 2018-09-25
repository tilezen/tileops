import boto3
from botocore.exceptions import ClientError


def find_repo(ecr, repo_name):
    try:
        result = ecr.describe_repositories(repositoryNames=[repo_name])
        repositories = result['repositories']
        if len(repositories) != 0:
            assert len(repositories) == 1
            return repositories[0]['repositoryUri']

    except ClientError as e:
        if e.response['Error']['Code'] != 'RepositoryNotFoundException':
            raise

    return None


def ensure_repo(ecr, repo_name):
    repo_uri = find_repo(ecr, repo_name)

    if repo_uri is None:
        response = ecr.create_repository(repositoryName=repo_name)
        repo_uri = response['repository']['repositoryUri']

    return repo_uri


def ensure_ecr(run_id):
    ecr = boto3.client('ecr')

    repo_names = (
        'meta-low-zoom-batch',
        'rawr-batch',
        'meta-batch',
        'missing-meta-tiles-write',
    )

    repo_uris = {}
    for repo_name in repo_names:
        full_name = 'tilezen/%s-%s' % (repo_name, run_id)
        repo_uris[repo_name] = ensure_repo(ecr, full_name)

    return repo_uris

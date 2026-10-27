"""Set up an agent-only deployment; never create or run sample workloads."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import yaml
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType
from smoke import ROOT, CONTEXT, LOCAL, PROJECT, client, identifier, save


def agent_config():
    config = yaml.safe_load((ROOT / 'databricks.yml').read_text())
    if set(config.get('resources', {}).get('jobs', {})) != {'discover_assess', 'execute_demo'} or 'agent' not in config.get('targets', {}):
        raise RuntimeError('Extract the agent-only release into a fresh directory. This command refuses to alter the existing dev/demo deployment.')


def bundle(context, action):
    variables = ','.join(f'{key}={context[key]}' for key in ('catalog', 'schema', 'app_name'))
    run = subprocess.run(['databricks', 'bundle', action, '--profile', context['profile'], '--target', 'agent', '--var', variables, '--output', 'json'], cwd=ROOT, capture_output=True, text=True)
    if run.returncode:
        raise RuntimeError(run.stderr or run.stdout)
    return json.loads(run.stdout) if run.stdout.strip() else {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'provision', 'validate', 'deploy'))
    parser.add_argument('--profile')
    parser.add_argument('--catalog')
    parser.add_argument('--schema')
    parser.add_argument('--app-name', default='migration-agent')
    args = parser.parse_args()
    agent_config()
    if args.action in ('check', 'provision'):
        if not args.profile or not args.catalog:
            parser.error('check/provision requires --profile and --catalog')
        identifier(args.catalog)
        context = {'profile': args.profile, 'catalog': args.catalog}
        w = client(context)
        catalog = w.catalogs.get(args.catalog)
        metastore = w.metastores.current()
        if args.action == 'check':
            print(json.dumps({'read_only': True, 'host': w.config.host, 'catalog': catalog.name, 'catalog_owner': catalog.owner, 'metastore_attached': bool(metastore.metastore_id), 'schemas': [s.name for s in w.schemas.list(args.catalog)], 'creation_permissions': 'Not proven by a read-only metadata check; ownership/effective grants must allow project provisioning.', 'resources_created': 0}, indent=2))
            return
        if not args.schema or not args.schema.startswith('migration_agent_jr_'):
            parser.error('provision requires a new schema starting with migration_agent_jr_')
        identifier(args.schema)
        if not re.fullmatch(r'[a-z][a-z0-9-]{2,62}', args.app_name):
            parser.error('Use a unique lowercase App name, 3-63 letters/digits/hyphens.')
        context.update(schema=args.schema, app_name=args.app_name, job_id='', classic_cluster_id='')
        if CONTEXT.exists():
            previous = json.loads(CONTEXT.read_text())
            if any(previous.get(k) != value for k, value in context.items()):
                raise RuntimeError('Existing local context belongs to another deployment. Use a fresh extracted directory.')
            context = previous
        try:
            schema = w.schemas.get(args.catalog + '.' + args.schema)
            if (schema.properties or {}).get('migration_agent_project') != PROJECT:
                raise RuntimeError('Refusing to reuse a schema without this project marker.')
        except NotFound:
            w.schemas.create(args.schema, args.catalog, properties={'migration_agent_project': PROJECT}, comment='Agent planning and isolated candidate evidence')
        for name in ('artifacts', 'plans'):
            try:
                volume = w.volumes.read(args.catalog + '.' + args.schema + '.' + name)
                if volume.comment != PROJECT:
                    raise RuntimeError('Refusing to reuse an unmarked project volume.')
            except NotFound:
                w.volumes.create(args.catalog, args.schema, name, VolumeType.MANAGED, comment=PROJECT)
        context['workspace_root'] = '/Workspace/Users/' + w.current_user.me().user_name + '/.bundle/workspace-migration-agent-jr/agent'
        save(context)
        print(json.dumps({'status': 'Project storage provisioned', 'sample_jobs_created': 0, 'jobs_run': 0}))
        return
    context = json.loads(CONTEXT.read_text())
    result = bundle(context, 'validate')
    if args.action == 'deploy':
        bundle(context, 'deploy')
        result = bundle(context, 'summary')
        jobs = result['resources']['jobs']
        context.update(scan_job_id=int(jobs['discover_assess']['id']), runner_job_id=int(jobs['execute_demo']['id']))
        app = result['resources']['apps']['migration_app']
        context.update(app_name=app['name'], app_url=app.get('url'))
        save(context)
        LOCAL.mkdir(exist_ok=True)
        (LOCAL / 'bundle-summary.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'action': args.action, 'target': 'agent', 'sample_jobs_created': 0, 'workload_runs_started': 0}, indent=2))


if __name__ == '__main__':
    main()

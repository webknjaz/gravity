#!/usr/bin/env python

import argparse
import json
import glob
import os
import shutil
import subprocess
import sys
import yaml

from collections.abc import Mapping


import requests

from bs4 import BeautifulSoup
from logzero import logger
from sh import find as shfind


DEVEL_URL = 'https://github.com/ansible/ansible.git'
DEVEL_BRANCH = 'devel'

VARDIR = os.environ.get('GRAVITY_VAR_DIR', '.cache')
COLLECTION_NAMESPACE = 'ansible-collection'
COLLECTION_PACKAGE_PREFIX = 'ansible-collection-'
COLLECTION_INSTALL_PATH = '/usr/share/ansible/collections/ansible_collections'

PLUGIN_EXCEPTION_PATHS = {'modules': 'lib/ansible/modules', 'module_utils': 'lib/ansible/module_utils'}


def _run_command(cmd=None, check_rc=True):
    logger.debug(cmd)
    if not isinstance(cmd, bytes):
        cmd = cmd.encode('utf-8')
    p = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    (so, se) = p.communicate()

    if check_rc and p.returncode != 0:
        raise RuntimeError(se)

    so = so.decode('utf-8')
    se = se.decode('utf-8')

    return (p.returncode, so, se)


def run_command(cmd=None, check_rc=True):
    (rc, so, se) = _run_command(cmd, check_rc)
    return {
        'rc': rc,
        'so': so,
        'se': se
    }


def checkout_repo(refresh=False):
    releases_dir = os.path.join(VARDIR, 'releases')
    devel_path = os.path.join(releases_dir, 'devel.git')

    if refresh and os.path.exists(devel_path):
        # TODO do we want/is it worth to use a git library instead?
        cmd = 'cd %s; git checkout %s; git pull' % (devel_path, DEVEL_BRANCH)
        rc, stdout, stderr = _run_command(cmd)

    if not os.path.exists(releases_dir):
        os.makedirs(releases_dir)

    if not os.path.exists(devel_path):
        cmd = 'git clone %s %s; cd %s; git checkout %s' % (DEVEL_URL, devel_path, devel_path, DEVEL_BRANCH)
        rc, stdout, stderr = _run_command(cmd)


def load_spec_file(spec_file):

    spec = {}
    with open(spec_file, 'rb') as spec_fpointer:
        # TODO: capture yamlerror?
        spec = yaml.safe_load(spec_fpointer)

    if not isinstance(spec, Mapping):
        sys.exit("Invalid format for spec file, expected a dictionary and got %s" % type(spec))
    elif not spec:
        sys.exit("Cannot use spec file, ended up with empty spec")

    return spec


def clean_extra_lines(rawtext):
    lines = rawtext.split('\n')

    imports_start = None
    imports_stop = None
    for idx, x in enumerate(lines):
        if imports_start is None:
            if x.startswith('from ') and not 'absolute_import' in x:
                imports_start = idx
                continue

        if not x:
            continue

        if x.startswith('from '):
            continue

        if imports_start and imports_stop is None:
            if x[0].isalnum():
                imports_stop = idx
                break

    empty_lines = [x for x in range(imports_start, imports_stop)]
    empty_lines = [x for x in empty_lines if not lines[x].strip()]

    if not empty_lines:
        return rawtext

    if len(empty_lines) == 1:
        return rawtext

    # keep 2 empty lines between imports and definitions
    if len(empty_lines) == 2 and (empty_lines[-1] - empty_lines[-2] == 1):
        return rawtext

    print(lines[imports_start:imports_stop])

    while empty_lines:
        try:
            print('DELETING: %s' % lines[empty_lines[0]])
        except IndexError as e:
            print(e)
            import epdb; epdb.st()
        del lines[empty_lines[0]]
        del empty_lines[0]
        empty_lines = [x-1 for x in empty_lines]
        if [x for x in empty_lines if x <= 0]:
            break

        if len(empty_lines) <= 2:
            break

        #import epdb; epdb.st()

    rawtext = '\n'.join(lines)
    return rawtext


def rewrite_doc_fragments(pdata, coll, spec, args):

    # fix the docs fragments
    # TODO: same issue than with module_utils, it assumes fragment resides in the same collection,
    # must use spec to find actual collection and then both rewrite extends line AND add collection
    # to depenencies of current collection.

    # TODO: use ansible-doc --json instead? plugin loader/docs directly?
    parsed = safe_eval(data)
    raw_docs = parsed.get('DOCUMENTATION')
    docs = yaml.load(raw_docs)

    for fragment in docs.get('extends_documentation_fragment', []):
        print('ignoring %s fragment, needs rewrite' % fragment)

    # TODO: this incorrectly assumed single fragment/string, when it is a list
    # extends_documentation_fragment: vmware.documentation\n'
    #    newfrag = '%s.%s.%s' % (args.namespace, coll, fragment)
    #    mdata = mdata.replace(
    #        'extends_documentation_fragment: ' + df,
    #        'extends_documentation_fragment: ' + ddf,
    #    )

        #TODO: update gdata if fragment is in diff collection


def rewrite_mod_utils(pdata, coll, spec, args):
    # ansible.module_utils.
    token = 'ansible.module_utils.'

    # TODO: this assumes the module_utils resides in the same collection, must use spec to find actual collection
    # and then both rewrite import AND add collection to depenencies of current collection.
    # ansible_collections.jctanner.cloud_vmware.module_utils.
    dlines = pdata.split('\n')
    for idx, x in enumerate(dlines):
        if not x.startswith('from '):
            continue
        if token in x:  # TODO actually lookup part after token in spec to find 'correct collection'
            exchange = 'ansible_collections.%s.%s.plugins.module_utils.' % (args.namespace, coll)
            newx = x.replace(token, exchange)

            #TODO: update gdata if module_util is in diff collection

            # now handle line length rules
            if len(newx) < 160 and ('(' not in x) and '\\' not in x:
                dlines[idx] = newx
                continue

            if '(' in x and ')' not in x:
                x = ''
                tonull = []
                for thisx in range(idx, len(mdlines)):
                    x += dlines[thisx]
                    tonull.append(thisx)
                    if ')' in dlines[thisx]:
                        break

                if len(tonull) > 1:
                    extralines = True
                for tn in tonull:
                    dlines[tn] = ''

            if '\\' in x:
                x = ''
                tonull = []
                for thisx in range(idx, len(dlines)):

                    if thisx != idx and dlines[thisx].startswith('from '):
                        break

                    print('add %s' % dlines[thisx])
                    x += dlines[thisx]
                    tonull.append(thisx)

                    if thisx != idx and (not dlines[thisx].strip() or dlines[thisx][0].isalnum()):
                        break
                    print('add %s' % dlines[thisx])

                if len(tonull) > 1:
                    extralines = True
                for tn in tonull:
                    dlines[tn] = ''

            # we have to use newlined imports for those that are >160 chars
            ximports = x[:]

            #if '(' in x and ')' not in x:
            #    import epdb; epdb.st()

            if si in ximports:
                ximports = ximports.replace(token, '')
            elif di in ximports:
                ximports = ximports.replace(exchange, '')
            ximports = ximports.replace('from', '')
            ximports = ximports.replace('import', '')
            ximports = ximports.replace('\\', '')
            ximports = ximports.replace('(', '')
            ximports = ximports.replace(')', '')
            ximports = ximports.split(',')
            ximports = [x.strip() for x in ximports if x.strip()]
            ximports = sorted(set(ximports))

            newx = 'from %s import (\n' % exchange
            for xi in ximports:
                newx += '    ' + xi + ',\n'
            newx += ')'
            dlines[idx] = newx

    data = '\n'.join(dlines)


def assemble_collections(spec, args):
    releasedir = os.path.join(args.vardir, 'releases')
    colbasedir = os.path.join(args.vardir, 'collections')
    metadir = os.path.join(args.vardir, 'meta')

    if args.refresh and os.path.exists(colbasedir):
        shutil.rmtree(colbasedir)

    # loop over specified collections
    for coll in spec.keys():

        requirements = set()
        cdir = os.path.join(colbasedir, 'ansible_collections', coll)

        if args.refresh and os.path.exists(cdir):
            shutil.rmtree(cdir)
        if not os.path.exists(cdir):
            os.makedirs(cdir)

        # create the data for galaxy.yml
        gdata = {
            'namespace': args.namespace,
            'name': coll,
            'version': '1.0.0',  # TODO: add to spec, args?
            'authors': None,
            'description': None,
            'license': None,
            'tags': None,
            'dependencies': None,
            'repository': None,
            'documentation': None,
            'homepage': None,
            'issues': None
        }

        # loop per plugin type in collection
        for plugin_type in spec[coll].keys():

            # get right plugin path
            if plugin_type not in PLUGIN_EXCEPTION_PATHS:
                base = os.path.join('lib', 'ansible', plugin_type)
            else:
                base = PLUGIN_EXCEPTION_PATHS[plugin_type]

            # ensure destinations exist
            tldr = os.path.join(cidr, base)
            if not os.path.exists(tldr):
                os.makedirs(tldr)
                with open(os.path.join(tldr, '__init__.py'), 'w') as f:
                    f.write('')

            # now actually process each plugin
            for plugin in spec[coll][plugin_type].keys():

                # TODO: currently requires 'full name of file', but should work w/o extension?
                src = os.path.join(args.vardir, base, plugin)
                dst = os.path.join(tldr, os.path.basename(plugin))

                # create and read copy for modifycation
                shutil.copy(src, dst)
                with open(dst, 'r') as f:
                    pdata = f.read()
                _pdata = pdata[:]

                # were any lines nullified?
                extralines = False

                rewrite_mod_utils(pdata, coll, spec, args)
                rewrite_doc_fragments(pdata, coll, spec, args)

                # clean too many empty lines
                if extralines:
                    data = clean_extra_lines(data)

                if data != _data:
                    logger.info('fixing imports in %s' % dst)
                    with open(dst, 'w') as f:
                        f.write(data)

                # process unit tests TODO: sanity? , integration?
                copy_unit_tests(plugin, coll, spec, args)

        # write collection metadata
        with open(os.path.join(cdir, 'galaxy.yml'), 'w') as f:
            f.write(yaml.dump(gdata, default_flow_style=False))


def copy_tests(plugin, coll, spec, args):

    # TODO: tests might also require rewriting imports, docfragments and even play/tasks,
    #  why i made functions above from preexisting code
    return

    # UNIT TESTS
    # need to fix these imports in the unit tests

    dst = os.path.join(plugin, 'test', 'unit')
    if not os.path.exists(dst):
        os.makedirs(dst)
    for uf in spec['units']:  # TODO: should we rely on spec or 'autofind' matching units of same name/type?
        fuf = os.path.join(args.vardir, 'test', 'units', uf)
        if os.path.isdir(fuf):
            #import epdb; epdb.st()

            fns = glob.glob('%s/*' % fuf)
            for fn in fns:
                if os.path.isdir(fn):
                    try:
                        shutil.copytree(fn, os.path.join(dst, os.path.basename(fn)))
                    except Exception as e:
                        pass
                else:
                    shutil.copy(fn, os.path.join(dst, os.path.basename(fn)))


        elif os.path.isfile(fuf):
            fuf_dst = os.path.join(dst, os.path.basename(fuf))
            shutil.copy(fuf, fuf_dst)

        cmd = 'find %s -type f -name "*.py"' % (dst)
        res = run_command(cmd)
        unit_files = sorted([x.strip() for x in res['so'].split('\n') if x.strip()])

        for unit_file in unit_files:
            # fix the module import paths to be relative
            #   from ansible.modules.cloud.vmware import vmware_guest
            #   from ...plugins.modules import vmware_guest

            depth = unit_file.replace(cdir, '')
            depth = depth.lstrip('/')
            depth = os.path.dirname(depth)
            depth = depth.split('/')
            rel_path = '.'.join(['' for x in range(-1, len(depth))])

            with open(unit_file, 'r') as f:
                unit_lines = f.readlines()
            unit_lines = [x.rstrip() for x in unit_lines]

            changed = False

            for module in module_names:
                for li,line in enumerate(unit_lines):
                    if line.startswith('from ') and line.endswith(module):
                        unit_lines[li] = 'from %s.plugins.modules import %s' % (rel_path, module)
                        changed = True

            if changed:
                with open(unit_file, 'w') as f:
                    f.write('\n'.join(unit_lines))
            #import epdb; epdb.st()


        list_of_targets = []  # TODO: same as above require from spec or find for ourselves?
        if list_of_targets:
            dst = os.path.join(cdir, 'test', 'integration', 'targets')
            if not os.path.exists(dst):
                os.makedirs(dst)
            for uf in v['targets']:
                fuf = os.path.join(args.vardir, 'test', 'integration', 'targets', uf)
                duf = os.path.join(dst, os.path.basename(fuf))
                if not os.path.exists(os.path.join(dst, os.path.basename(fuf))):
                    try:
                        shutil.copytree(fuf, duf)
                    except Exception as e:
                        import epdb; epdb.st()

                # set namespace for all module refs
                cmd = 'find %s -type f -name "*.yml"' % (duf)
                res = run_command(cmd)
                yfiles = res['so'].split('\n')
                yfiles = [x.strip() for x in yfiles if x.strip()]

                for yf in yfiles:
                    with open(yf, 'r') as f:
                        ydata = f.read()
                    _ydata = ydata[:]

                    for module in v['modules']:
                        msrc = os.path.basename(module)
                        msrc = msrc.replace('.py', '')
                        msrc = msrc.replace('.ps1', '')
                        msrc = msrc.replace('.ps2', '')

                        mdst = '%s.%s.%s' % (args.namespace, coll, msrc)

                        if msrc not in ydata or mdst in ydata:
                            continue

                        #import epdb; epdb.st()
                        ydata = ydata.replace(msrc, mdst)

                    # fix import_role calls?
                    #tasks = yaml.load(ydata)
                    #import epdb; epdb.st()

                    if ydata != _ydata:
                        logger.info('fixing module calls in %s' % yf)
                        with open(yf, 'w') as f:
                            f.write(ydata)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--spec', '--spec_file', required=True, dest='spec_file',
                        help='spec YAML file that describes how to organize collections')
    parser.add_argument('-n', '--ns', '--namespace', dest='namespace', default=COLLECTION_NAMESPACE,
                        help='target namespace for resulting collections')
    parser.add_argument('-r', '--refresh', action='store_true', dest='refresh', default=False,
                        help='force refreshing local Ansible checkout')
    parser.add_argument('-t', '--target-dir', dest='vardir', default=VARDIR,
                        help='target directory for resulting collections and rpm')

    args = parser.parse_args()

    # required, so we should always have
    spec = load_spec_file(args.spec_file)

    checkout_repo(args.refresh)

    # doeet
    assemble_collections(spec, args)


if __name__ == "__main__":
    main()

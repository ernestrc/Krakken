#!/usr/bin/env python2.7
import os
import subprocess
from subprocess import *
import shutil
import sys

PROJECTS = {'microservice1': 'microservice.App',
            'microservice2': 'microservice.Mainclass'}

TEMPLATE = 'deploy/microservice/Dockerfile'

SCALA = '2.11'

DOCKER_REPO = 'myDockerRepo'


def parseVersion():
    with open('version.sbt', 'r') as versionSbt:
        v = versionSbt.read().split(':=')[-1].strip().strip('\"')
        print 'Version is {}'.format(v)
        return v


def buildDockerfile(pr, v):
    pro_dockerfile = './target/Dockerfile-' + pr
    shutil.copy(TEMPLATE, pro_dockerfile)
    with open(pro_dockerfile) as f:
        a = f.read().replace(
            '@SERVICE', pr).replace(
            '@SCALA', SCALA).replace(
            '@MAINCLASS', PROJECTS[pr]).replace(
            '@VERSION', v).replace(
            '@PWD', os.getcwd())

    with open(pro_dockerfile, 'w') as f:
        f.write(a)

    print 'Copied {} to {}'.format(TEMPLATE, pro_dockerfile)
    return pro_dockerfile


def buildImage(df, pr, v):
    tag = '{}/{}:{}'.format(DOCKER_REPO, pr, v)
    cmd = 'docker build --file={} -t {} .'.format(df, tag)
    p = subprocess.Popen(cmd, shell=True)
    return p.wait()


def pushImage(pr, v):
    cmd = 'docker push {}/{}:{}'.format(DOCKER_REPO, pr, v)
    p = subprocess.Popen(cmd, shell=True)
    return p.wait()


def checkStatus(status, msg):
    if status != 0:
        print msg
        sys.exit(2)


def parseCommitId():
    cmd = 'git rev-parse HEAD'
    p = subprocess.Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    status = p.wait()
    if status != 0:
        raise Exception('There was an error when parsing last commit id')
    print 'Parsed commit id {}. Using it as image tag...'.format(stdout)
    return stdout.strip()


def main(release=False):
    p = subprocess.Popen('sbt assembly', shell=True)
    status = p.wait()
    status = 0
    print 'Finished building jars. Status code is %s' % (status)

    version = parseVersion()
    image_version = version if release else parseCommitId()

    for project in PROJECTS.keys():

        dockerfile = buildDockerfile(project, version)

        build_status = buildImage(dockerfile, project, image_version)

        checkStatus(build_status,
                    '\nThere was problem when building image for {}'.format(
                        project))

        push_status = pushImage(project, image_version)

        checkStatus(push_status,
                    '\nThere was a problem when pushing image!')


if __name__ == '__main__':
    usage = '''
This script assembles (via sbt assembly) all the projects in your build.sbt
and uses a template Dockerfile to build and push a docker image for every project.
It will use the last commit id as the image tag unless specified otherwise with
--release flag.


USAGE: build.py [options]

OPTIONS:

    --release               Uses version in version.sbt as the docker image tag

    --help, -h              Display this message

'''

    def printUsage(msg):
        print msg
        print usage
        sys.exit(1)

    SUPPORTED = ['--release']
    flags = [flag for flag in sys.argv if flag.startswith('-')]
    [printUsage('Unrecognized flag ' + flag) for flag in flags
     if flag.split('=')[0] not in SUPPORTED]

    args = [arg for arg in sys.argv if not arg.startswith('-')]

    if '--help' in flags or '-h' in flags:
        printUsage('')

    RELEASE = True if '--release' in flags else False

    main(RELEASE)

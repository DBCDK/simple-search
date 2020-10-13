#!groovy

def workerNode = "xp-build-i01"

pipeline {
	agent { label workerNode }
	environment {
		ARTIFACTORY_LOGIN = credentials("artifactory_login")
		DOCKER_TAG = "${env.BRANCH_NAME}-${env.BUILD_NUMBER}"
		GITLAB_PRIVATE_TOKEN = credentials("ai-gitlab-api-token")
	}
	stages {
		stage("test") {
			agent {
				docker {
					label workerNode
					image "docker.dbc.dk/build-env"
					alwaysPull true
				}
			}
			steps {
				sh """#!/usr/bin/env bash
					set -xe
					rm -rf env test-results.xml
					# Install with --system-site-packages to get access to the globally installed pytest module
					python3 -m venv env --system-site-packages
					source env/bin/activate
					pip install .
					# Invoke pytest like this to make python add the current directory to the pythonpath:
					# https://docs.pytest.org/en/latest/pythonpath.html#pytest-import-mechanisms-and-sys-path-pythonpath
					python3 -m pytest --junitxml=test-results.xml
					make-build-info
				"""
				junit "test-results.xml"
				stash includes: "src/simple_search/_build_info.py", name: "build-stash"
			}
		}
		stage("upload wheel package") {
			agent {
				docker {
					label workerNode
					image "docker.dbc.dk/build-env"
					alwaysPull true
				}
			}
			when {
				branch "master"
			}
			steps {
				unstash "build-stash"
				sh """#!/usr/bin/env bash
					set -xe
					rm -rf dist
					make-build-info
					python3 setup.py egg_info --tag-build=${env.BUILD_NUMBER} bdist_wheel
					twine upload -u $ARTIFACTORY_LOGIN_USR -p $ARTIFACTORY_LOGIN_PSW --repository-url https://artifactory.dbc.dk/artifactory/api/pypi/pypi-dbc dist/*
				"""
			}
		}
		stage("docker build") {
			steps {
				unstash "build-stash"
				script {
					image = docker.build(
						"docker-xp.dbc.dk/simple-search:${DOCKER_TAG}", "--pull --no-cache .")
					image.push()
					if(env.BRANCH_NAME == "master") {
						image.push("latest")
					}
				}
			}
		}
		stage("update staging version number") {
			agent {
				docker {
					label workerNode
					image "docker.dbc.dk/build-env"
					alwaysPull true
				}
			}
			when {
				branch "master"
			}
			steps {
				sh "set-new-version simple-search-1-0.yml ${env.GITLAB_PRIVATE_TOKEN} ai/simple-search-secrets ${env.DOCKER_TAG} -b staging"
				build job: "ai/simple-search/simple-search-deploy/staging", wait: true

				sh "set-new-version simple-search-1-0.yml ${env.GITLAB_PRIVATE_TOKEN} ai/simple-search-secrets ${env.DOCKER_TAG} -b staging-search-evaluation"
				build job: "ai/simple-search/simple-search-deploy/staging-search-evaluation", wait: true

				sh "set-new-version simple-search-bibdk-1-0.yml ${env.GITLAB_PRIVATE_TOKEN} ai/simple-search-secrets ${env.DOCKER_TAG} -b staging-bibdk"
				build job: "ai/simple-search/simple-search-deploy/staging-bibdk", wait: true
			}
		}
		stage("validate staging") {
			agent {
				docker {
					label workerNode
					image "docker.dbc.dk/build-env"
					alwaysPull true
				}
			}
			when {
				branch "master"
			}
			steps {
				sh "webservice_validation.py http://simple-search-1-0.mi-staging.svc.cloud.dbc.dk deploy/validation.yml"
				sh "webservice_validation.py http://simple-search-evaluation-1-0.mi-staging.svc.cloud.dbc.dk deploy/validation.yml"
				sh "webservice_validation.py http://simple-search-bibdk-1-0.mi-staging.svc.cloud.dbc.dk deploy/validation.yml"
			}
		}
		stage("update prod version number") {
			agent {
				docker {
					label workerNode
					image "docker.dbc.dk/build-env"
					alwaysPull true
				}
			}
			when {
				branch "master"
			}
			steps {
				sh "set-new-version simple-search-1-0.yml ${env.GITLAB_PRIVATE_TOKEN} ai/simple-search-secrets ${env.DOCKER_TAG} -b prod"
				build job: "ai/simple-search/simple-search-deploy/prod", wait: true

				sh "set-new-version simple-search-1-0.yml ${env.GITLAB_PRIVATE_TOKEN} ai/simple-search-secrets ${env.DOCKER_TAG} -b prod-search-evaluation"
				build job: "ai/simple-search/simple-search-deploy/prod-search-evaluation", wait: true

				sh "set-new-version simple-search-bibdk-1-0.yml ${env.GITLAB_PRIVATE_TOKEN} ai/simple-search-secrets ${env.DOCKER_TAG} -b prod-bibdk"
				build job: "ai/simple-search/simple-search-deploy/prod-bibdk", wait: true
			}
		}
		stage("validate prod") {
			agent {
				docker {
					label workerNode
					image "docker.dbc.dk/build-env"
					alwaysPull true
				}
			}
			when {
				branch "master"
			}
			steps {
				sh "webservice_validation.py http://simple-search-1-0.mi-prod.svc.cloud.dbc.dk deploy/validation.yml"
				sh "webservice_validation.py http://simple-search-evaluation-1-0.mi-prod.svc.cloud.dbc.dk deploy/validation.yml"
				sh "webservice_validation.py http://simple-search-bibdk-1-0.mi-prod.svc.cloud.dbc.dk deploy/validation.yml"
			}
		}
	}
}

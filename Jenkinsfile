#!groovy

def workerNode = "xp-build-i01"

pipeline {
	agent { label workerNode }
	environment {
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
				"""
				junit "test-results.xml"
			}
		}
		stage("docker build") {
			steps {
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
			}
		}
	}
}

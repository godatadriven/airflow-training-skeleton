# Airflow training skeleton

This project serves as a skeleton project on which Airflow training participants build exercises.

At the start of the training, you will receive a Google Cloud Platform project, with a pre-configured Cloud Composer environment (Google's managed Airflow).

Training participants generally like taking their code home after the training, so we provide them with a skeleton Airflow project on GitHub. In order to push and deploy, we kindly ask the participants to setup their own CI because we don't know the GitHub handles of participants beforehand. However the setup is just a few clicks, described below.

## Setup CI instructions

A CI pipeline is included with the project (`cloudbuild.yml`), which defines steps to execute after each push. However, there are variables which must be entered before applying the CI pipeline. The CI pipeline tests and deploys your code to Google Cloud Composer.

1. Fork the repository.
2. In the GCP console, go to your GCP project, and browse to Cloud Build.
3. Go to Build triggers.
4. Click "Create trigger".
5. Select source GitHub and click Continue.
6. You have to allow Google Cloud Platform to access your repositories.
7. In Cloud Build, you can now select your repositories. Select this forked repository and check the consent box.
8. Under `Build configuration`, select `cloudbuild.yaml` and click Create trigger at the bottom.

Done. The just configured build trigger now watches changes on your repository.

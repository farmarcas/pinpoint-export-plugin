# Pinpoint Export Plugin

Export events to Amazon Pinpoint on ingestion. Archive your data, or simply free it up for other kinds of analysis, by integrating export right into your event processing pipeline.

## Installation

1. Access PostHog's **Plugins** page from the sidebar.
1. To perform installation either:
    1. go to the **Repository** tab, find this plugin, and **Install** it,
    1. or go to the **Advanced** tab and **Fetch and install** the following URL in the **Install from GitHub, GitLab or npm** section:  
        `http://github.com/PostHog/posthog-plugin-geoip`.
1. Configure the plugin by entering your AWS credentials and S3 bucket details.
1. Watch events roll into S3!

## Obtaining AWS Credentials

1. Log in to [AWS](https://console.aws.amazon.com/).
1. Open [Pinpoint](https://console.aws.amazon.com/pinpoint/) in the AWS console and create a project to send events.
1. Open [IAM](https://console.aws.amazon.com/iam/home) and create a new policy to allow access to this bucket.
    1. Open "Policies" and click "Create policy"
    1. On the "Visual Editor" tab, click "Choose a service" and select "Pinpoint"
    1. Under "Actions" select
        1. "Write" -> "PutEvents"
    1. Specify your bucket name and choose "any" for the object name, so the ARN looks something like this: `arn:aws:mobiletargeting:{Region}:{AccountId}:apps/{AppId}/*`
    1. On the "Visual Editor" tab, click "Choose a service" and select "Pinpoint"
    1. Under "Actions" select
        1. "Write" -> "UpdateEndpoint"
    1. Specify your bucket name and choose "any" for the object name, so the ARN looks something like this: `arn:aws:mobiletargeting:{Region}:{AccountId}:apps/{AppId}/*`
    1. On the "Visual Editor" tab, click "Choose a service" and select "Mobile Analytics"
    1. Under "Actions" select
        1. "Write" -> "PutEvents"
    1. Under "Resources" select "Specific" and click "object" -> "All resources"
    1. Click "Next" until you end up on the "Review Policy" page
    1. Give it a name
1. Open [IAM](https://console.aws.amazon.com/iam/home) and create a new user who uses this policy
    1. Click "Users" -> "Add User"
    1. Specify a name and choose "Programmatic access"
    1. Click "Next" 
    1. Select "Attach existing policies directly"
    1. Select the policy you had just created
    1. Click "Next" until you reach the "Create user" button. Click that as well.
    1. Make sure to copy your "Access key" and "Secret access key". The latter will not be shown again.
1. Install the plugin in PostHog and fill in the "Access key", "Secret access key", "Pinpoint region" and "Application Id" fields. Adjust other parameters as needed.

## Memory Usage

To vastly increase export throughput, this plugin batches events in memory before uploading them to S3. Upload frequency (every minute by default) and maximum upload 
size (1 MB by default) can be configured when the plugin is installed.

You should make sure to keep these numbers reasonable to avoid running out of memory on your server. Note that the values apply to **each** concurrent plugin server thread.

## Questions?

### Thanks
This project was based on ["S3 Export Plugin"](https://github.com/posthog/s3-export-plugin) so thanks to all team.

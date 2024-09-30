import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { PondLambdaStack } from "../lib/pond-lambda-stack";

const app = new cdk.App();
const useLocalStack = app.node.tryGetContext("use_local_stack") === true;

new PondLambdaStack(app, "PondLambdaStack", {
  env: useLocalStack
    ? { account: "000000000000", region: "us-east-1" }
    : {
        account: process.env.CDK_DEFAULT_ACCOUNT,
        region: process.env.CDK_DEFAULT_REGION,
      },
});

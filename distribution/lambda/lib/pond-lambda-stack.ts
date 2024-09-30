import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";
import { getAssetPath } from "./utils";

export class PondLambdaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const pondPlannerLambda = new lambda.Function(this, "PondPlannerLambda", {
      runtime: lambda.Runtime.PROVIDED_AL2,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(
        getAssetPath("pond", "target", "lambda", "pond-planner"),
      ),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
    });

    const pondDucklingLambda = new lambda.Function(this, "PondDucklingLambda", {
      runtime: lambda.Runtime.PROVIDED_AL2,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(
        getAssetPath("pond", "target", "lambda", "pond-duckling"),
      ),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
    });
  }
}

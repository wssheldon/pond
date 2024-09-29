import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";

export class PondLambdaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const pondPlannerLambda = new lambda.Function(this, "PondPlannerLambda", {
      runtime: lambda.Runtime.PROVIDED_AL2,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(
        path.join(
          __dirname,
          "../../pond/pond-planner/target/lambda/pond-planner/",
        ),
      ),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
    });

    const pondDucklingLambda = new lambda.Function(this, "PondDucklingLambda", {
      runtime: lambda.Runtime.PROVIDED_AL2,
      handler: "bootstrap",
      code: lambda.Code.fromAsset(
        path.join(
          __dirname,
          "../../pond/pond-duckling/target/lambda/pond-duckling/",
        ),
      ),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
    });
  }
}

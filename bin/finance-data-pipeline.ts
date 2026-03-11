#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { FinanceIngestionStack } from "../lib/finance-data-pipeline-stack";

const app = new cdk.App();
new FinanceIngestionStack(app, "FinanceIngestionStack", {});

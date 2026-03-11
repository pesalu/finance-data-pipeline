# Finance Data Pipeline

This project downloads large financial datasets using AWS Step Functions,
Lambda, S3, Secrets Manager, and SSM Parameter Store.

Deploy with:
```
cdk bootstrap
cdk deploy
```

Project structure:

```
finance-data-pipeline/
│
├── bin/
│   └── finance-data-pipeline.ts
│
├── lib/
│   └── finance-data-pipeline-stack.ts
│
├── lambda/
│   ├── getChunkList/
│   │   ├── index.py
│   │   └── requirements.txt
│   │
│   ├── downloadChunk/
│   │   ├── index.py
│   │   └── requirements.txt
│   │
│   └── finalizeJob/
│       ├── index.py
│       └── requirements.txt
│
├── parameters/
│   ├── base-url.txt
│   └── symbols.json
│
├── cdk.json
├── package.json
├── tsconfig.json
└── README.md
```
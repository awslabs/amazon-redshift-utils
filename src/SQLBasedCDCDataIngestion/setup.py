import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="dlpoc",
    version="0.0.1",

    description="An empty CDK Python app",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"": "dlpoc"},
    packages=setuptools.find_packages(where="dlpoc"),

    install_requires=[
        "aws-cdk.core==1.72.0",
        "aws_cdk.aws_redshift==1.72.0",
        "aws_cdk.aws_rds==1.72.0",
        "aws_cdk.aws_dms==1.72.0",
        "aws_cdk.aws_glue==1.72.0",
        "aws_cdk.aws_ec2==1.72.0",
        "aws_cdk.aws_secretsmanager==1.72.0",
        "aws_cdk.aws_glue==1.72.0",
        "aws_cdk.aws_lambda==1.72.0",
        "aws_cdk.custom_resources==1.72.0",
        "aws_cdk.aws_lambda_python==1.72.0",
        "aws_cdk.aws_events==1.72.0",
        "aws_cdk.aws_sns==1.72.0",
        "aws_cdk.aws_events_targets==1.72.0"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)

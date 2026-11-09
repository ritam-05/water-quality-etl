@echo off
echo === Starting Automated ETL Deployment ===

echo [1/4] Cleaning previous builds...
if exist deploy_package rmdir /s /q deploy_package
if exist processor.zip del processor.zip
mkdir deploy_package

echo [2/4] Packaging dependencies and code...
pip install pg8000==1.30.5 scramp==1.4.4 python-dateutil==2.8.2 -t deploy_package >nul
copy src\processor.py deploy_package\ >nul
cd deploy_package
tar -a -c -f ..\processor.zip *
cd ..

echo [3/4] Uploading new code to AWS Lambda...
aws lambda update-function-code --function-name water-quality-processor --zip-file fileb://processor.zip >nul

echo [WAITING] Allowing AWS 10-15 seconds to unlock the function...
aws lambda wait function-updated --function-name water-quality-processor

echo [4/4] Updating environment configuration...
aws lambda update-function-configuration --function-name water-quality-processor --environment file://update_env.json >nul

echo === Deployment Complete! ===
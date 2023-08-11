#!/bin/bash
# To zip all lambda functions and create artifacts to be used in the deployment
cd ./lambdas
for i in ./*; do
  printf "$i";
  cd $i;
  zip -r "../${i}.zip" ./*;
  cd ../;
done
cd ../

# To zip etl_job_modules package
zip -r etl_job_modules.zip etl_job_modules/

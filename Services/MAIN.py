from AMAZON_RDS import Amazon_RDS
from AMAZON_S3 import Amazon_S3
from AZ_Storage import Azure_S
from AMAZON_EBS import Amazon_EBS
from AMAZON_COMPUTE import Amazon_Compute
from AZ_Compute import Azure_Compute

print(".................................................WELCOME......................................................")
argument=input("Please Enter the OfferCode:")
def switch_demo(argument):
    switcher={"Amazon_S3" :"Amazon_S3('AmazonS3')",
              "Amazon_EC2":"Amazon_Compute()",
              "Amazon_EBS":"Amazon_EBS()",
              "Amazon_RDS":"Amazon_RDS('AmazonRDS')",
              "Azure_Storage":"Azure_S()",
              "Az_Compute":"Azure_Compute()"
    }
    a=switcher.get(argument,"invalid")
    exec(a)
if __name__ =="__main__":

    switch_demo(argument)

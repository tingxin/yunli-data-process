import boto3
import json

def get_ri_billing_id():
    # Create Pricing client
    pricing_client = boto3.client('pricing')
    # Search for the billing ID using the RI SKU
    response = pricing_client.get_products(
        ServiceCode='AmazonEC2',
        MaxResults=100
    )

    # Retrieve the billing ID from the pricing response
    index = 0
    for item in response['PriceList']:
     
        if item.find('Reservation') < 0:
            continue
        
        index +=1
        print(index)
        product = json.loads(item)
        
        for key in product['terms']['OnDemand']:
            priceDimensions  = product['terms']['OnDemand'][key]['priceDimensions']
            for detail_key in priceDimensions:
                priceDimension = priceDimensions[detail_key]
                description = priceDimension['description']
                print(f"账单ID ={key}\n 账单产品ID ={detail_key}\n 详细内容：{description}")

  
get_ri_billing_id()


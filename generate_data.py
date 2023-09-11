from faker import Faker
import json
import random


fake = Faker()

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

def generate_data(num_records):
    data = []

    for i in range(num_records):
        coupon = fake.random_element([None, fake.random_int(min=config["coupon_min"], max=config["coupon_max"])])
        shipping_info = fake.random_element([None, fake.city()])

        record={
            "TransactionID":fake.uuid4(),
            "Timestamp":fake.past_datetime(start_date="-1y"),
            "UserID":fake.uuid4(),
            "TransactionType":fake.random_element(config["transaction_type_options"]),
            "TransactionAmount":round(random.uniform(10, 1000), 2),
            "Currency":fake.random_element(config["currency_options"]),
            "PaymentMethod":fake.random_element(config["payment_method_options"]),
            "PaymentStatus":fake.random_element(config["payment_status_options"]),
            "MerchantID":random.randint(config["payment_gateway_id_min"], config["payment_gateway_id_max"]),
            "MerchantName":fake.company(),
            "TransactionDescription":fake.sentence(),
            "TransactionSource":fake.random_element(config["referring_url_options"]),
            "SessionID":fake.uuid4(),
            "UserIPAddress":fake.ipv4(),
            "UserAgent":fake.user_agent(),
            "DeviceType":'Laptop',
            "DeviceOS":fake.random_element(config["device_os_options"]),
            "DeviceBrowser":fake.random_element(config["device_browser_options"]),
            "DeviceScreenResolution":fake.random_element(config["device_resolution_options"]),
            "TimeZone":fake.timezone(),
            "Location":fake.random_element(config["location_options"]),
            "PaymentGatewayID":fake.uuid4(),
            "PaymentProcessorID":fake.uuid4(),
            "AuthorizationCode":fake.random_element(config["authorization_code_options"]),
            "TransactionResponseCode":fake.random_element(config["transaction_response_code_options"]),
            "FraudScore":random.randint(config["fraud_score_min"], config["fraud_score_max"]),
            "RiskFlag":fake.random_element(config["risk_flag_options"]),
            "AuthenticationMethod":fake.random_element(config["authentication_method_options"]),
            "Coupon":coupon,
            "ShippingInformation":shipping_info,
            "ReferringURL":fake.url(),
            "PromoCode":fake.random_int(min=config["promo_code_min"], max=config["promo_code_max"]),
        }

        data.append(record)
    return data

if __name__=='__main__':
    num_records=int(input("Enter the number of records: "))
    data=generate_data(num_records)
    json_object=json.dumps(data,indent=4,default=str)
    with open('sample_data1.json','w') as json_file:
        json_file.write(json_object)
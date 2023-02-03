import random

def generate_phone_number():
  country_code = "+34"
  primer_numero = str(6)
  
  primeros_3_digits = str(random.randint(1, 999)).zfill(3)
  segundos_3_digits = str(random.randint(1, 9999)).zfill(3)
  terceros_3_digits = str(random.randint(1, 9999)).zfill(3)

  
  phone_number = country_code + " " + primer_numero + segundos_3_digits + terceros_3_digits
  return phone_number

print(generate_phone_number())
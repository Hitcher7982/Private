import pandas as pd
import xml.etree.ElementTree as ET

temp_list = []
tree = ET.parse(r"C:\Users\SUMEAGAR\OneDrive - Capgemini\Desktop\XMLtoCSV\m_dq_to_dw_resource_dim.xml")
root = tree.getroot()

key_list = [
    "SOURCEFIELD",
    "TARGETFIELD",
    "TRANSFORMATION",
    "TRANSFORMFIELD",
    "TABLEATTRIBUTE",
    "GROUP",
    "INSTANCE",
    "CONNECTOR",
    "TARGETLOADORDER"
]
for i in root.iter("TARGETLOADORDER"):
    temp_list.append(i.attrib)

df = pd.DataFrame(temp_list)
print(df.to_string())

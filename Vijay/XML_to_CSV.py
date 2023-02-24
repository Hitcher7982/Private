import pandas as pd
import xml.etree.ElementTree as ET


# Reading XML file
tree = ET.parse("m_dq_to_dw_resource_dim.xml")
root = tree.getroot()


def get_dataframe(key):
    """
    This function is used to get Pandas DataFrame with 2 level of keys.
    param key: First level of key name.
    :return: Pandas DataFrame with proper parsing of XML keys.
    """

    # Declaring an Pandas DataFrame
    final_df = pd.DataFrame([i.attrib for i in root.iter(key)])

    return final_df


writer = pd.ExcelWriter(r"output.xlsx", engine="xlsxwriter")
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

for item in key_list:
    get_dataframe(item).to_excel(writer, sheet_name=item, index=False)

writer.save()

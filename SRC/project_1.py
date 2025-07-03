import xml.etree.ElementTree as ET
import pandas as pd

def parse_informatica_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    # Data structures to hold the lineage information
    lineage = []
    
    # Extract sources
    sources = {}
    for source in root.findall(".//SOURCE"):
        source_name = source.get('NAME')
        if source_name not in sources:
            sources[source_name] = []
        for field in source.findall(".//SOURCEFIELD"):
            field_name = field.get('NAME')
            sources[source_name].append(field_name)
    
    # Extract transformations
            transformations = {}
            for transformation in root.findall(".//TRANSFORMATION"):
                trans_name = transformation.get('NAME')
                trans_type = transformation.get('TYPE')
                transformations[trans_name] = {
                    'type': trans_type,
                    'fields': []
                    }
                for field in transformation.findall(".//TRANSFORMFIELD"):
                    field_name = field.get('NAME')
                    transformations[trans_name]['fields'].append(field_name)
    
    # Extract instances
                    instances = {}
                    for instance in root.findall(".//INSTANCE"):
                        inst_name = instance.get('NAME')
                        trans_name = instance.get('TRANSFORMATION_NAME')
                        instances[inst_name] = trans_name
    
    # Extract connectors and build lineage
                        for connector in root.findall(".//CONNECTOR"):
                            from_field = connector.get('FROMFIELD')
                            to_field = connector.get('TOFIELD')
                            from_instance = connector.get('FROMINSTANCE')
                            to_instance = connector.get('TOINSTANCE')
                            from_trans = instances[from_instance]
                            to_trans = instances[to_instance]
                            lineage.append([from_trans, from_field, to_trans, to_field])
    
    return sources, transformations, lineage

def build_lineage_table(sources, transformations, lineage):
    data = []
    
    # Add sources to data
    for source_name, fields in sources.items():
        for field in fields:
            data.append([source_name, field, '', '', '', '', ''])
    
    # Add transformations and lineage to data
    for from_trans, from_field, to_trans, to_field in lineage:
        trans_type = transformations[to_trans]['type'] if to_trans in transformations else 'Target Definition'
        data.append([from_trans, from_field, '->', to_trans, to_field, trans_type, ''])
    
    df = pd.DataFrame(data, columns=['Source/Transformation', 'Field', '', 'Target Transformation', 'Target Field', 'Transformation Type', ''])
    
    return df

def save_lineage_to_excel(df, output_file):
    df.to_excel(output_file, index=False)

if __name__ == "__main__":
    xml_file = 'SDE_WC_AR_BRXACT_FS'  # Replace with your XML file path
    sources, transformations, lineage = parse_informatica_xml(xml_file)
    lineage_df = build_lineage_table(sources, transformations, lineage)
    save_lineage_to_excel(lineage_df, 'data_lineage1.xlsx')

print("Data lineage has been saved to data_lineage.xlsx")
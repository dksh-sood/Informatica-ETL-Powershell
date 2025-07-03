import xml.etree.ElementTree as ET
import pandas as pd
import re

def parse_mapping_xml(mapping_file):
    try:
        tree = ET.parse(mapping_file)
        root = tree.getroot()
        
        sources = {}
        targets = {}
        transformations = []

        # Extract source information
        for source in root.findall(".//SOURCE"):
            source_name = source.get('NAME')
            if source_name not in sources:
                sources[source_name] = []
            for field in source.findall(".//SOURCEFIELD"):
                field_name = field.get('NAME')
                sources[source_name].append(field_name)

        # Extract target information
        for target in root.findall(".//TARGET"):
            target_name = target.get('NAME')
            if target_name not in targets:
                targets[target_name] = []
            for field in target.findall(".//TARGETFIELD"):
                field_name = field.get('NAME')
                targets[target_name].append(field_name)

        # Extract transformation information
        for transformation in root.findall(".//TRANSFORMATION"):
            transformation_name = transformation.get('NAME')
            transformation_type = transformation.get('TYPE')
            for field in transformation.findall(".//TRANSFORMFIELD"):
                field_name = field.get('NAME')
                port_type = field.get('PORTTYPE')
                transformations.append({
                    'Transformation Name': transformation_name,
                    'Transformation Type': transformation_type,
                    'Field Name': field_name,
                    'Port Type': port_type
                })

        return sources, targets, transformations
    except Exception as e:
        print(f"Error parsing mapping XML: {e}")
        return {}, {}, []

def parse_repo_metadata(repo_metadata_file):
    try:
        tree = ET.parse(repo_metadata_file)
        root = tree.getroot()
        
        repo_info = {}

        # Extract repository information
        repo_info['Repository Name'] = root.find(".//REPOSITORY").get('NAME')
        repo_info['Database Type'] = root.find(".//REPOSITORY").get('DATABASETYPE')
        repo_info['Folder Name'] = root.find(".//FOLDER").get('NAME')
        repo_info['Description'] = root.find(".//FOLDER").get('DESCRIPTION')

        return repo_info
    except Exception as e:
        print(f"Error parsing repository metadata XML: {e}")
        return {}

def parse_session_log(session_log_file):
    try:
        with open(session_log_file, 'r') as file:
            log_content = file.read()
        
        session_info = {}

        # Extract session start and end times
        session_info['Session Start Time'] = re.search(r'Session start time: (.+)', log_content).group(1)
        session_info['Session End Time'] = re.search(r'Session end time: (.+)', log_content).group(1)
        
        # Extract session status
        session_info['Status'] = re.search(r'Session status: (.+)', log_content).group(1)
        
        # Extract performance metrics
        session_info['Total Rows Processed'] = re.search(r'Total Rows Processed: (\d+)', log_content).group(1)
        session_info['Rows Inserted'] = re.search(r'Rows Inserted: (\d+)', log_content).group(1)
        session_info['Rows Rejected'] = re.search(r'Rows Rejected: (\d+)', log_content).group(1)

        return session_info
    except Exception as e:
        print(f"Error parsing session log: {e}")
        return {}

def create_data_lineage(sources, targets, transformations):
    data_lineage = []

    # Example logic to map sources to targets through transformations
    for target_name, target_fields in targets.items():
        for target_field in target_fields:
            lineage_entry = {
                'Source Name': '',
                'Source Field': '',
                'Transformation Name': '',
                'Transformation Type': '',
                'Target Name': target_name,
                'Target Field': target_field
            }
            # Add logic to map source fields to target fields through transformations
            for transformation in transformations:
                if transformation['Port Type'] == 'OUTPUT' and transformation['Field Name'] == target_field:
                    lineage_entry['Transformation Name'] = transformation['Transformation Name']
                    lineage_entry['Transformation Type'] = transformation['Transformation Type']
                    # Find corresponding source field
                    for source_name, source_fields in sources.items():
                        if transformation['Field Name'] in source_fields:
                            lineage_entry['Source Name'] = source_name
                            lineage_entry['Source Field'] = transformation['Field Name']
                            break
            data_lineage.append(lineage_entry)

    return data_lineage

def save_to_excel(data_lineage, session_info, repo_info, output_file):
    try:
        df = pd.DataFrame(data_lineage)
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Data Lineage', index=False)
            
            # Add session info and repo info as additional sheets
            session_df = pd.DataFrame([session_info])
            session_df.to_excel(writer, sheet_name='Session Info', index=False)
            
            repo_df = pd.DataFrame([repo_info])
            repo_df.to_excel(writer, sheet_name='Repo Info', index=False)
        
        print(f"Data lineage has been successfully written to {output_file}")
    except Exception as e:
        print(f"Error saving to Excel: {e}")

# File paths
mapping_file = 'Python_tutorials_mapping.XML'
repo_metadata_file = 'Python_tutorials_RepoMetadata.XML'
session_log_file = 'Python_tutorials_session_log.txt'
output_file = 'data_lineage_single_tab_updated.xlsx'

# Parse files
sources, targets, transformations = parse_mapping_xml(mapping_file)
repo_info = parse_repo_metadata(repo_metadata_file)
session_info = parse_session_log(session_log_file)

# Create data lineage
data_lineage = create_data_lineage(sources, targets, transformations)

# Save to Excel
save_to_excel(data_lineage, session_info, repo_info, output_file)
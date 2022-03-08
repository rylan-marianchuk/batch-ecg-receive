import numpy as np
import pandas as pd
import json


def write_ident_to_sql(arr, conx):
    """
    Write this batch of extracted identified attributes to sqlite table
    :param arr: (list of lists)
    :param conx: (sqlite connection)
    :return:
    """
    return

def send_json_to_socket(arr, url):
    """

    :param arr:
    :param url:
    :return:
    """
    return

def get_acq_datetimes(puid, conx):
    """
    Used for duplicate entry detection
    :param puid:
    :param conx:
    :return:
    """

def get_rhrn2puid_mapping(CIROC_PATIENT_PATH):
    DB_csv = pd.read_csv(CIROC_PATIENT_PATH, dtype=str)
    RHRNs = list(DB_csv["RHRN"])
    PUIDs = list(DB_csv["PID"])
    return {RHRNs[i] : PUIDs[i] for i in range(DB_csv.shape[0])}

def write_rhrn2puid_mapping(mapping, CIROC_PATIENT_PATH):
    pd.DataFrame({"RHRN": list(mapping.keys()), "PUID": list(mapping.values())},
                 columns=["RHRN", "PUID"]).to_csv(CIROC_PATIENT_PATH, index=False)
    return


def get_json_str(euid, puid, acquisitionDate, xml_str):
    """

    :param euid:
    :param puid:
    :param acquisitionDate:
    :param xml_str:
    :return:
    """
    json_str = json.dumps({
        "EUID" : euid,
        "PUID" : puid,
        "AcquisitionDate" : acquisitionDate,
        "XML" : xml_str
    })
    return json_str

def deidentify(tree, identified_attr):
    """

    :param identified_attr:
    :param tree:
    :return:
    """
    identified_elements = []
    for attribute in identified_attr:
        found = tree.find(attribute)
        if found is None:
            identified_elements.append("NULL")
            continue
        identified_elements.append(found.text)
        found.text = ' '

    return tree, identified_elements


def getFormattedDateTime(tree):
    d = tree.find('.//AcquisitionDate').text
    t = tree.find('.//AcquisitionTime').text
    if None in (d, t):
        raise Exception("Could not find Date or Time of Acquisition")
    # Shift date string to YYYY-MM-DD
    # FORMAT YYYY-MM-DD_HH:MM:SS
    return d.split("-")[-1] + "-" + d[:5] + "_" + t

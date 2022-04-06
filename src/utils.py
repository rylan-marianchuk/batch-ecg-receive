import math
import time

import pandas as pd
import json
import h5py
import torch
import base64
import array
import ctypes
from ctypes import *
import numpy as np
from src.sqlite_column_iter import SqliteColumnIter

up = torch.nn.Upsample(scale_factor=2, mode='linear', align_corners=False)
cosSim = torch.nn.CosineSimilarity(dim=0)

def send_json_to_socket(arr, url):
    """

    :param arr:
    :param url:
    :return:
    """
    return


def generate_summary(buid, sqlwrapper):
    """

    :param buid:
    :return:
    """
    return


def is_duplicate(puid, acqDate, acqTime, sqlwrapper):
    """
    Used for duplicate entry detection
    :param puid:
    :param sqlwrapper:
    :return:
    """
    res = sqlwrapper.conx.execute("SELECT * FROM Decoder WHERE PUID=:puid AND AcquisitionDate=:acqDate AND AcquisitionTime=:acqTime",
                {"puid": puid, 'acqDate':acqDate, 'acqTime':acqTime})
    if len(res.fetchall()) == 0:
        return False
    return True

def parse_statement_text(tree):
    """
    Obtain the physician provided statement text within the xml
    :param tree:
    :return:
    """
    diagnosis_xml = tree.find(".//Diagnosis").findall(".//DiagnosisStatement")
    diagnosis_stmt = ""
    for e in diagnosis_xml:
        diagnosis_stmt += e.find(".//StmtText").text + "\n"

    orig_diagnosis_xml = tree.find(".//OriginalDiagnosis").findall(".//DiagnosisStatement")
    orig_diagnosis_stmt = ""
    for e in orig_diagnosis_xml:
        orig_diagnosis_stmt += e.find(".//StmtText").text + "\n"

    return diagnosis_stmt, orig_diagnosis_stmt


def parse_qrs_measurements(tree):
    """
    Obtain all MUSE exported measurements of the ECG within the xml
    Obtain the generated QRS detection output embedded in the xml

    note return order matters, each return entry is a sqlite column!

    :param tree: (ElementTree) of the ecg xml
    :return: (ndarray) shape=(heart beats, 2) with row[i] = type, time (as time-th sample),
             (int) number of QRS complexes,
             (int) GlobalRR,
             (int) QTRGGR
             (float) avgRR
    """

    qrs = tree.find(".//QRSTimesTypes").findall(".//QRS")
    qrs_numpy = np.zeros(shape=(len(qrs), 2), dtype=np.short)
    for i,e in enumerate(qrs):
        type = int(e.find(".//Type").text)
        time = int(e.find(".//Time").text)
        qrs_numpy[i] = np.array([type, time], dtype=np.short)
    globalRR = int(tree.find(".//QRSTimesTypes").find(".//GlobalRR").text)
    qtrggr = int(tree.find(".//QRSTimesTypes").find(".//QTRGGR").text)
    avgRR = np.diff(qrs_numpy[:, 1]).sum() / qrs_numpy.shape[0]

    orig_measurements = tree.find(".//OriginalRestingECGMeasurements")

    ventricular_rate = int(orig_measurements.find(".//VentricularRate").text)
    atrial_rate = int(orig_measurements.find(".//AtrialRate").text)
    pr_interval = int(orig_measurements.find(".//PRInterval").text)
    qrs_duration = int(orig_measurements.find(".//QRSDuration").text)


    return qrs_numpy, qrs_numpy.shape[0], globalRR, qtrggr, avgRR


def parse_filters(wvfm_tree):
    """

    :param tree:
    :return:
    """
    fs = int(wvfm.find("SampleBase").text)
    hpf = int(wvfm.find("HighPassFilter").text)
    lpf = int(wvfm.find("LowPassFilter").text)
    ac = int(wvfm.find("ACFilter").text)
    return fs, hpf, lpf, ac




def write_lead_features(signal_container, subbatch_progress, lead_euids, buid, wvfm_sqlwrapper):
    """

    :return:
    """
    # Signal count
    s = subbatch_progress*8
    resCL, resHE, resAC, res20flat = invoke_gpu(signal_container, s)
    resAC = get_autocorr_sim(signal_container, s)
    sqlite_column_iter = SqliteColumnIter((lead_euids, [buid]*s, [0, 1, 2, 3, 4, 5, 6, 7]*subbatch_progress, res20flat, resCL, resHE, resAC))
    wvfm_sqlwrapper.batch_insert("WaveformFeatures", iter(sqlite_column_iter))
    return

def writeh5(tree, euid, puid, h5path):
    """
    Decode the waveforms in the xml, upsample if needed, save them in a contiguous tensor to h5
    :param tree: (ElementTree) of the ecg xml
    :param euid: (str) encounter id
    :param puid: (str) patient id
    :param h5path: (str) /path/to/write/h5s/to/
    :return: (tensor) shape=(40,000) one dimensional array of the ecg, each lead appended end to end
    """
    rhythm_wfrm = tree.findall('.//Waveform')[1]
    rhythm_leads = rhythm_wfrm.findall("LeadData")
    # Sampling frequency
    fs = int(rhythm_wfrm.find("SampleBase").text)

    ECG_lead_dict = {}

    # Assume xml always has 8 leads
    for lead_ind in range(8):
        lead_xml = rhythm_leads[lead_ind]
        encodedStr = lead_xml.find("WaveFormData").text
        lead_ID = lead_xml.find("LeadID").text
        to_decode = base64.b64decode(encodedStr)
        T = torch.tensor(array.array('h', to_decode), dtype=torch.float32)
        # Upsample to 250 if needed
        if fs == 250:
            T = up(T.unsqueeze(0).unsqueeze(0)).flatten()
        ECG_lead_dict[lead_ID] = T

    # Create the contiguous tensor, literals acceptable here because this is standard lead and signal length
    ecg = torch.zeros(5000*8, dtype=torch.float32)

    for i, key in enumerate(("I", "II", "V1", "V2", "V3", "V4", "V5", "V6")):
        ecg[i*5000:(i+1)*5000] = ECG_lead_dict[key]

    h5 = h5py.File(h5path + euid + "_" + puid + ".h5", 'w')
    h5.create_dataset("ECG", data=ecg)
    h5.close()
    return ecg


def get_rhrn2puid_mapping(CIROC_PATIENT_PATH):
    DB_csv = pd.read_csv(CIROC_PATIENT_PATH, dtype=str)
    RHRNs = list(DB_csv["RHRN"])
    PUIDs = list(DB_csv["PUID"])
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
        "AcquisitionDate": acquisitionDate,
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
        found = tree.find(".//" + attribute)
        if found is None:
            identified_elements.append("NULL")
            continue
        identified_elements.append(found.text)
        found.text = ' '

    return tree, identified_elements


def getFormattedDateTime(tree):
    """

    :param tree:
    :return:
    """
    d = tree.find('.//AcquisitionDate').text
    t = tree.find('.//AcquisitionTime').text
    if None in (d, t):
        raise Exception("Could not find Date or Time of Acquisition")
    # Shift date string to YYYY-MM-DD
    # FORMAT YYYY-MM-DD_HH:MM:SS
    return d.split("-")[-1] + "-" + d[:5] + "_" + t, d, t


def invoke_gpu(signal_container, SIGNALS):
    """

    :param signal_container: (tensor)
    :return:
    """
    dll = ctypes.CDLL("./src/wvfm_features.so", mode=ctypes.RTLD_GLOBAL)
    get_wvfm_features_gpu = dll.GetWvfmFeaturesGPU
    get_wvfm_features_gpu.argtypes = [POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_float), POINTER(c_int), c_size_t]


    ecg_container_p = signal_container.numpy().ctypes.data_as(POINTER(c_float))
    resCL = np.zeros(SIGNALS).astype("float32")
    resCL_p = resCL.ctypes.data_as(POINTER(c_float))

    resHE = np.zeros(SIGNALS).astype("float32")
    resHE_p = resHE.ctypes.data_as(POINTER(c_float))

    resAC = np.zeros(SIGNALS).astype("float32")
    resAC_p = resAC.ctypes.data_as(POINTER(c_float))

    res20flat = np.zeros(SIGNALS).astype("int32")
    res20flat_p = res20flat.ctypes.data_as(POINTER(c_int32))

    get_wvfm_features_gpu(ecg_container_p, resCL_p, resHE_p, resAC_p, res20flat_p, SIGNALS)
    return resCL, resHE, resAC, res20flat


def get_autocorr_sim(signal_container, SIGNALS):
    """

    :param signal_container:
    :param SIGNALS:
    :return:
    """
    seg_size = 1250
    segs = 4
    nlags = 50
    resAC = np.zeros(SIGNALS)
    for x in range(SIGNALS):
        ACFs = torch.zeros(segs, nlags+1)
        signal = signal_container[x * 5000: (x+1) * 5000]
        for i in range(segs):
            segment = signal[i*seg_size:(i+1)*seg_size]
            demeaned = segment - segment.mean()
            Frf = np.fft.fft(demeaned, n=2560)
            acov = np.fft.ifft(Frf * np.conjugate(Frf))[:seg_size] / (1250 * np.ones(1250))
            acov = acov.real
            acf = acov[:nlags+1] / acov[0]
            ACFs[i] = torch.from_numpy(acf)

        pairwiseM = torch.zeros(4, 4)

        for i, j in [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]:
            A1 = ACFs[i]
            A2 = ACFs[j]
            similarity = cosSim(A1, A2)
            if not (0 <= similarity <= 1):
                similarity = min(similarity, 1.00)
            theta = math.acos(similarity)
            pairwiseM[i, j] = theta
            pairwiseM[j, i] = theta

        resAC[x] = torch.sum(pairwiseM, dim=1).sum().item()
    return resAC
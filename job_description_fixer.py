__author__ = 'complynx'


def split(val, separator=","):
    v_arr = val.split(separator)

    for i, v in enumerate(v_arr):
        v_arr[i] = parse_value(v)

    return v_arr


def get_nulls(val):
    return val if val != "NULL" else None

key_fix = {
    'PandaID': 'id',  # it is job id, not PanDA one
    'transformation': 'command',  # making it more convenient
    'jobPars': 'command_parameters',  # -.-

}

arrays = []

skip_keys = [  # these are tested elsewhere
    'inFiles', "ddmEndPointIn", "destinationSE", "dispatchDBlockToken", "realDatasetsIn", "prodDBlocks", "fsize",
    "checksum", "outFiles", "ddmEndPointOut", "fileDestinationSE", "dispatchDBlockTokenForOut",
    "destinationDBlockToken", "realDatasets", "destinationDblock", "logGUID"
]


def is_float(val):
    try:
        float(val)
        return True
    except ValueError:
        return False


def is_long(s):
    if s[0] in ('-', '+'):
        return s[1:].isdigit()
    return s.isdigit()


def parse_value(value):
    if not isinstance(value, basestring):
        return value
    if is_long(value):
        return long(value)
    if is_float(value):
        return float(value)
    return get_nulls(value)


def get_input_files(description):
    files = {}
    if description['inFiles'] and description['inFiles'] != "NULL":
        in_files = split(description["inFiles"])
        ddm_endpoint = split(description["ddmEndPointIn"])
        destination_se = split(description["destinationSE"])
        data_block_token = split(description["dispatchDBlockToken"])
        datasets = split(description["realDatasetsIn"])
        data_blocks = split(description["prodDBlocks"])
        size = split(description["fsize"])
        c_sum = split(description["checksum"])

        for i, f in enumerate(in_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_data_block_token": data_block_token[i],
                    "dataset": datasets[i],
                    "data_block": data_blocks[i],
                    "size": size[i],
                    "checksum": c_sum[i]
                }
    return files


def fix_log(description, files):
    if description["logFile"] and description["logFile"] != "NULL":
        if description["logGUID"] and description["logGUID"] != "NULL" and description["logFile"] in\
                    files:
            files[description["logFile"]]["guid"] = description["logGUID"]

    return files


def get_output_files(description):
    files = {}
    if description['outFiles'] and description['outFiles'] != "NULL":
        out_files = split(description["outFiles"])
        ddm_endpoint = split(description["ddmEndPointOut"])
        destination_se = split(description["fileDestinationSE"])
        data_block_token = split(description["dispatchDBlockTokenForOut"])
        destination_data_block_token = split(description["destinationDBlockToken"])
        datasets = split(description["realDatasets"])
        data_blocks = split(description["destinationDblock"])

        for i, f in enumerate(out_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_data_block_token": data_block_token[i],
                    "destination_data_block_token": destination_data_block_token[i],
                    "dataset": datasets[i],
                    "data_block": data_blocks[i]
                }

    return fix_log(description, files)


def description_fixer(description):
    fixed = {}
    for key in description:
        value = description[key]

        if key in key_fix:
            key = key_fix[key]

        fixed['input_files'] = get_input_files(description)
        fixed['output_files'] = get_output_files(description)

        if key not in skip_keys:
            if key in arrays:
                fixed[key] = split(value)
            else:
                fixed[key] = parse_value(value)
    return fixed

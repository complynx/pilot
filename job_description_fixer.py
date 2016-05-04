"""
This file is a standalone job description converter from the old description to a proposed one.
"""

import re
import logging
import json
import numbers

logging.basicConfig()
log = logging.getLogger()
DEBUG = False
CONSOLE = False


def camel_to_snake(name):
    """
    Changes CamelCase to snake_case, used by python.

    :param name: name to change
    :return: name in snake_case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_camel(snake_str):
    """
    Changes snake_case to firstLowCamelCase, used by python.

    :param name: name to change
    :return: name in snake_case
    """
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


def split(val, separator=","):
    """
    Splits comma separated values and parses them.

    :param val: values to split
    :param separator: comma or whatever
    :return: parsed values
    """
    v_arr = val.split(separator)

    for i, v in enumerate(v_arr):
        v_arr[i] = parse_value(v)

    return v_arr


def get_nulls(val):
    """
    Makes every "NULL" None.
    :param val: string or whatever
    :return: val or None if val is "NULL"
    """
    return val if val != "NULL" else None


key_fix = {
    'PandaID': 'job_id',  # it is job id, not PanDA
    'transformation': 'command',  # making it more convenient
    'jobPars': 'command_parameters',  # -.-
    'coreCount': 'cores_number',
    'prodUserID': 'user_dn',
    'prodSourceLabel': 'label',  # We don't have any other labels in there. And this is The Label, or just label
    'homepackage': 'home_package',  # lowercase, all of a sudden
    "nSent": 'throttle',  # as it's usage says
    'minRamCount': 'minimum_ram',  # reads better
    'maxDiskCount': 'maximum_disk_space',  # what does "count" mean? Partitions number? HDD's? Maybe number of disks
    # been used from the first installation of OS on that node?
    'attemptNr': 'attempt_number',  # bad practice to strip words API needs to be readable
    'prodDBlockToken': 'prod_dblock_token',  # camel_to_snake makes d_block, which is right, but heavier
}

arrays = []

skip_keys = [  # these are fixed elsewhere
               'inFiles', "ddmEndPointIn", "destinationSE", "dispatchDBlockToken", "realDatasetsIn", "prodDBlocks",
               "fsize",
               "checksum", "outFiles", "ddmEndPointOut", "fileDestinationSE", "dispatchDBlockTokenForOut",
               "destinationDBlockToken", "realDatasets", "destinationDblock", "logGUID", "scopeIn", "scopeOut",
               "scopeLog"
               ]

skip_new_keys = [  # these are fixed elsewhere
                   'input_files', "output_files"
                   ]
key_unfix = {
    'job_id': 'PandaID',
    'command': 'transformation',
    'command_parameters': 'jobPars',
    'cores_number': 'coreCount',
    'user_dn': 'prodUserID',
    'label': 'prodSourceLabel',
    'home_package': 'homepackage',
    "throttle": 'nSent',
    'minimum_ram': 'minRamCount',
    'maximum_disk_space': 'maxDiskCount',
    'attempt_number': 'attemptNr',
    'prod_dblock_token': 'prodDBlockToken',
    'job_definition_id': 'jobDefinitionID',
    'task_id': 'taskID',
    'guid': 'GUID',
    'jobset_id': 'jobsetID',
    'status_code': 'StatusCode',
}

key_explicit_strings = [
]


def is_float(val):
    """
    Test floatliness of the value.
    :param val: string or whatever
    :return: True if the value may be converted to Float
    """
    try:
        float(val)
        return True
    except ValueError:
        return False


def is_long(s):
    """
    Test value to be convertable to integer.
    :param s: string or whatever
    :return: True if the value may be converted to Long
    """
    if not isinstance(s, basestring):
        try:
            long(s)
            return True
        except ValueError:
            return False

    if s[0] in ('-', '+'):
        return s[1:].isdigit()
    return s.isdigit()


def parse_value(value):
    """
    Tries to parse value as number or None. If some of this can be done, parsed value is returned. Otherwise returns
    value without parsing.

    :param value:
    :return: mixed
    """
    if not isinstance(value, basestring):
        return value
    if is_long(value):
        return long(value)
    if is_float(value):
        return float(value)
    return get_nulls(value)


def get_input_files(description):
    """
    Extracts input files from the description.
    :param description:
    :return: file list
    """
    log.info("fixing input files in description")
    files = {}
    if description['inFiles'] and description['inFiles'] != "NULL":
        in_files = split(description["inFiles"])
        ddm_endpoint = split(description["ddmEndPointIn"])
        destination_se = split(description["destinationSE"])
        dblock_token = split(description["dispatchDBlockToken"])
        datasets = split(description["realDatasetsIn"])
        dblocks = split(description["prodDBlocks"])
        size = split(description["fsize"])
        c_sum = split(description["checksum"])
        scope = parse_value(description["scopeIn"])

        for i, f in enumerate(in_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_dblock_token": dblock_token[i],
                    "dataset": datasets[i],
                    "dblock": dblocks[i],
                    "size": size[i],
                    "checksum": c_sum[i],
                    'scope': scope
                }
    return files


def fix_log(description, files):
    """
    Fixes log file description in output files (changes GUID and scope).
    :param description:
    :param files: output files
    :return: fixed output files
    """
    log.info("modifying log-specific values in a log file description")
    if description["logFile"] and description["logFile"] != "NULL":
        if description["logGUID"] and description["logGUID"] != "NULL" and description["logFile"] in \
                files:
            files[description["logFile"]]["guid"] = description["logGUID"]
            files[description["logFile"]]["scope"] = description["scopeLog"]

    return files


def get_output_files(description):
    """
    Extracts output files from the description.
    :param description:
    :return: output files
    """
    log.info("fixing output files in description")
    files = {}
    if description['outFiles'] and description['outFiles'] != "NULL":
        out_files = split(description["outFiles"])
        ddm_endpoint = split(description["ddmEndPointOut"])
        destination_se = split(description["fileDestinationSE"])
        dblock_token = split(description["dispatchDBlockTokenForOut"])
        destination_dblock_token = split(description["destinationDBlockToken"])
        datasets = split(description["realDatasets"])
        dblocks = split(description["destinationDblock"])
        scope = parse_value(description["scopeOut"])

        for i, f in enumerate(out_files):
            if f is not None:
                files[f] = {
                    "ddm_endpoint": ddm_endpoint[i],
                    "storage_element": destination_se[i],
                    "dispatch_dblock_token": dblock_token[i],
                    "destination_dblock_token": destination_dblock_token[i],
                    "dataset": datasets[i],
                    "dblock": dblocks[i],
                    "scope": scope
                }

    return fix_log(description, files)


def set_logger(logger):
    global log
    if isinstance(logger, logging.Logger):
        log = logger
    else:
        log = logging.getLogger()


def debug(msg):
    if DEBUG:
        log.debug(msg)


def stringify_weird(arg):
    if arg is None:
        return "NULL"
    if isinstance(arg, numbers.Number):
        return arg
    return str(arg)


def join(arr):
    return ",".join(str(stringify_weird(x)) for x in arr)


def join_input_files(unfixed, input_files):
    in_files = []
    ddm_endpoint = []
    destination_se = []
    dblock_token = []
    datasets = []
    dblocks = []
    size = []
    c_sum = []
    scope = None

    for i in input_files:
        in_files.append(i)
        ddm_endpoint.append(input_files[i]['ddm_endpoint'])
        destination_se.append(input_files[i]['storage_element'])
        dblock_token.append(input_files[i]['dispatch_dblock_token'])
        datasets.append(input_files[i]['dataset'])
        dblocks.append(input_files[i]['dblock'])
        size.append(input_files[i]['size'])
        c_sum.append(input_files[i]['checksum'])
        scope = input_files[i]['scope']  # in old description all files are in one scope, so we assume this

    unfixed['inFiles'] = join(in_files)
    unfixed['ddmEndPointIn'] = join(ddm_endpoint)
    unfixed['destinationSE'] = join(destination_se)
    unfixed['dispatchDBlockToken'] = join(dblock_token)
    unfixed['realDatasetsIn'] = join(datasets)
    unfixed['prodDBlocks'] = join(dblocks)
    unfixed['fsize'] = join(size)
    unfixed['checksum'] = join(c_sum)
    unfixed['scopeIn'] = stringify_weird(scope)

    return unfixed


def unfix_log_parameters(unfixed, log_file):
    log.info("Extracting log-specific variables")
    unfixed["logGUID"] = log_file["guid"]
    unfixed["scopeLog"] = log_file["scope"]
    return unfixed


def join_output_files(unfixed, output_files, log_file):
    out_files = []
    ddm_endpoint = []
    destination_se = []
    dblock_token = []
    destination_dblock_token = []
    datasets = []
    dblocks = []
    scope = None

    for i in output_files:
        out_files.append(i)
        ddm_endpoint.append(output_files[i]['ddm_endpoint'])
        destination_se.append(output_files[i]['storage_element'])
        dblock_token.append(output_files[i]['dispatch_dblock_token'])
        destination_dblock_token.append(output_files[i]['destination_dblock_token'])
        datasets.append(output_files[i]['dataset'])
        dblocks.append(output_files[i]['dblock'])
        if i != log_file:
            scope = output_files[i]['scope']  # in old description all files are in one scope, so we assume this

    unfixed['outFiles'] = join(out_files)
    unfixed['ddmEndPointOut'] = join(ddm_endpoint)
    unfixed['fileDestinationSE'] = join(destination_se)
    unfixed['dispatchDBlockTokenForOut'] = join(dblock_token)
    unfixed['realDatasets'] = join(datasets)
    unfixed['destinationDBlockToken'] = join(destination_dblock_token)
    unfixed['destinationDblock'] = join(dblocks)
    unfixed['scopeOut'] = stringify_weird(scope)

    return unfix_log_parameters(unfixed, output_files[log_file])


def description_fixer(description, logger=None):
    """
    Parses the description and changes it into more readable and usable way. For example, extracts all the files and
    makes a structure of them.
    :param description:
    :param logging.Logger(logger): logger to use. Default logger otherwise.
    :return: fixed description
    """
    if logger is not None:
        set_logger(logger)

    log.info("Fixing description...")

    fixed = {}
    if isinstance(description, basestring):
        description = json.loads(description)

    debug("Loaded description: " + json.dumps(description, indent=4, sort_keys=True))

    if "PandaID" not in description:  # already fixed
        log.info("Description seem to be fixed already.")
        return description

    console_info("Extracting files")
    fixed['input_files'] = get_input_files(description)
    console_info("input_files fixed")
    fixed['output_files'] = get_output_files(description)
    console_info("output_files fixed")

    for key in description:
        value = description[key]

        if key not in skip_keys:
            old_key = key
            if key in key_fix:
                key = key_fix[key]
            else:
                key = camel_to_snake(key)

            if key in arrays:
                fixed[key] = split(value)
            else:
                fixed[key] = parse_value(value)

            console_info(old_key + " -> " + key + " | " + str(value) + " -> " + str(fixed[key]))
        else:
            console_info(key + " skipped")

    debug("Fixed description: " + json.dumps(fixed, indent=4, sort_keys=True))

    return fixed


def console_info(msg):
    if CONSOLE:
        log.info(msg)


def description_oldifier(description, logger=None):
    """
    Parses the description and changes it into old one.
    :param description:
    :param logging.Logger(logger): logger to use. Default logger otherwise.
    :return: old description
    """
    if logger is not None:
        set_logger(logger)

    log.info("Oldifying description...")

    unfixed = {}
    if isinstance(description, basestring):
        description = json.loads(description)

    debug("Loaded description: " + json.dumps(description, indent=4, sort_keys=True))

    if "PandaID" in description:  # already unfixed
        log.info("Description seem to be old enough.")
        return description

    console_info("unfixing files")
    unfixed = join_input_files(unfixed, description['input_files'])
    console_info("input_files unfixed")
    unfixed = join_output_files(unfixed, description['output_files'], description['log_file'])
    console_info("output_files unfixed")
    debug(json.dumps(unfixed, indent=4, sort_keys=True))

    for key in description:
        value = description[key]

        if key not in skip_new_keys:
            old_key = key
            if key in key_unfix:
                key = key_unfix[key]
            else:
                key = snake_to_camel(key)

            if type(value) is list:
                unfixed[key] = join(value)
            else:
                unfixed[key] = stringify_weird(value)

            if key in key_explicit_strings:
                unfixed[key] = str(unfixed[key])

            console_info(old_key + " -> " + key + " | " + str(value) + " -> " + str(unfixed[key]))
        else:
            console_info(key + " skipped")

    debug("Fixed description: " + json.dumps(unfixed, indent=4, sort_keys=True))

    return unfixed


if __name__ == "__main__":
    import argparse
    import sys
    import os

    CONSOLE = True

    arg_parser = argparse.ArgumentParser(description="This simple script fixes job description.")
    arg_parser.add_argument('-i', "--input", type=argparse.FileType('r'), default=sys.stdin,
                            help='Get job description from <file>. If not set, stdin is used.',
                            metavar=os.path.join('your', 'input.json'))
    arg_parser.add_argument('-o', "--output", type=argparse.FileType('w'), default=sys.stdout,
                            help='Save new job description into <file>. If not set, stdout is used.',
                            metavar=os.path.join('your', 'output.json'))
    arg_parser.add_argument('-S', "--silent", action='store_true',
                            help='Suppress all the warnings and errors.')
    arg_parser.add_argument('-R', "--revert", action='store_true',
                            help='Don\'t fix. Instead, do the opposite.')
    arg_parser.add_argument("--verbose", action='store_true',
                            help='Talk as much as possible. If used with -S, ignored.')
    arg_parser.add_argument("--DEBUG", action='store_true',
                            help='Sets debug flag to true.')
    args = arg_parser.parse_args(sys.argv[1:])

    if args.DEBUG:
        log.setLevel(0)
        DEBUG = True

    if args.verbose:
        log.setLevel(logging.INFO)

    if args.silent:
        log.setLevel(logging.CRITICAL)

    log.info("Log level %d" % log.getEffectiveLevel())

    log.info("loading file")
    try:
        description = json.load(args.input)
    except:
        log.error("Could not parse file. Exiting.")
        raise

    log.info("parsing description")
    try:
        if args.revert:
            fixed = description_oldifier(description)
        else:
            fixed = description_fixer(description)
    except:
        log.error("Could not fix description.")
        raise

    log.info("saving file")
    try:
        json.dump(fixed, args.output, indent=4, sort_keys=True)
    except:
        log.error("Could not save fixed description.")
        raise

import os
import numpy as np
import pandas as pd
import glob

from nipype.interfaces.base import TraitedSpec, BaseInterface, Bunch, isdefined
from nipype.interfaces.traits_extension import traits # , File
from nipype.interfaces.io import DataGrabberInputSpec, DataGrabber
from nipype.utils.filemanip import list_to_filename
import nipype.pipeline.engine as pe

import paramiko
import sys, os
from os.path import join as pjoin, split as psplit, exists as pexists
from distutils.dir_util import mkpath

import execnet
import getpass

def remote_glob(dspec, glob_string):
    '''
    dspec is a dict of username, hostname
    '''
    gw = execnet.makegateway('ssh=%s@%s' % (dspec['username'], dspec['hostname']))
    channel = gw.remote_exec("""
            import sys, os, glob
            channel.send(glob.glob('{glob_string}'))
    """.format(glob_string = glob_string))
    return channel.receive()

def download_from_sftp(remote_filepath_list, LOCAL_CACHE_BASE_DIR, HOSTNAME, PORT, USERNAME, PRIVATE_KEY, PRINTSTATUS = True):

    ## ssh = paramiko.SSHClient()
    ## ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ## ssh.connect(_hostname, username = _username, pkey = PRIVATE_KEY)
    ## ssh.exec_command('ls -l "%s"' % GLOB_STRING)

    print((HOSTNAME, PORT))
    transport = paramiko.Transport((HOSTNAME, PORT))
    ## transport.connect(username = _username, password = _password)
    transport.connect(username = USERNAME, pkey = PRIVATE_KEY)
    sftp = paramiko.SFTPClient.from_transport(transport)

    out = []
    for remote_filepath in remote_filepath_list:
        local_filepath = pjoin(LOCAL_CACHE_BASE_DIR, remote_filepath.lstrip(os.path.sep))
        local_filedir  = psplit(local_filepath)[0]
        if not pexists(local_filedir):
            mkpath(local_filedir)
        if not pexists(local_filepath):
            if PRINTSTATUS:
                print " " + "_" * (len(local_filepath)+8)
                print "/ <--    %s" % remote_filepath
                print "\\" + "____--> %s" % local_filepath
            sftp.get(remote_filepath, local_filepath)
        out.append(local_filepath)
    return out

## TODO
## make server config non-mandatory
## and make the grabber fallback to local filesystem
## (i.e. passthrough to base datagrabber) when not supplied
class RemoteDataGrabberInputSpec(DataGrabberInputSpec):
    # host configuration
    hostname = traits.Str(desc='server hostname',)
    port = traits.Int(22, usedefault = True, )
    username = traits.Str(desc='username used for SFTP to the remote host',)
    passphrase = traits.Str(desc='passphrase for private key, if applicable')
    local_cache_directory = traits.Str(desc='base directory on the local host, where the files mirrored from the remote will be stored',)

    # datagrabber base configuration
    base_directory = traits.Str(desc='base directory on the REMOTE host',)

class RemoteDataGrabber(DataGrabber):
    input_spec = RemoteDataGrabberInputSpec

    def _map_to_local_cache(self, filelist):
        PRIVATE_KEY = paramiko.RSAKey.from_private_key_file(os.path.expanduser('~/.ssh/id_rsa'), password = self.inputs.passphrase)
        return download_from_sftp(filelist, self.inputs.local_cache_directory,
                self.inputs.hostname, self.inputs.port,
                self.inputs.username, PRIVATE_KEY)

    # copied from https://github.com/nipy/nipype/blob/d75713d1269e5f5ce9a650055ba793b90d358333/nipype/interfaces/io.py#L368
    def _list_outputs(self):
        # infields are mandatory, however I could not figure out how to set 'mandatory' flag dynamically
        # hence manual check
        if self._infields:
            for key in self._infields:
                value = getattr(self.inputs, key)
                if not isdefined(value):
                    msg = "%s requires a value for input '%s' because it was listed in 'infields'" % \
                        (self.__class__.__name__, key)
                    raise ValueError(msg)

        outputs = {}
        for key, args in self.inputs.template_args.items():
            outputs[key] = []
            template = self.inputs.template
            if hasattr(self.inputs, 'field_template') and \
                    isdefined(self.inputs.field_template) and \
                    key in self.inputs.field_template:
                template = self.inputs.field_template[key]
            if isdefined(self.inputs.base_directory):
                template = os.path.join(
                    os.path.abspath(self.inputs.base_directory), template)
            else:
                template = os.path.abspath(template)
            if not args:
                if self.inputs.hostname and self.inputs.username:
                    filelist = self._map_to_local_cache(remote_glob(dict(username = self.inputs.username, hostname = self.inputs.hostname), template))
                else:
                    filelist = glob.glob(template)
                if len(filelist) == 0:
                    msg = 'Output key: %s Template: %s returned no files' % (
                        key, template)
                    if self.inputs.raise_on_empty:
                        raise IOError(msg)
                    else:
                        warn(msg)
                else:
                    if self.inputs.sort_filelist:
                        filelist.sort()
                    outputs[key] = list_to_filename(filelist)
            for argnum, arglist in enumerate(args):
                maxlen = 1
                for arg in arglist:
                    if isinstance(arg, str) and hasattr(self.inputs, arg):
                        arg = getattr(self.inputs, arg)
                    if isinstance(arg, list):
                        if (maxlen > 1) and (len(arg) != maxlen):
                            raise ValueError('incompatible number of arguments for %s' % key)
                        if len(arg) > maxlen:
                            maxlen = len(arg)
                outfiles = []
                for i in range(maxlen):
                    argtuple = []
                    for arg in arglist:
                        if isinstance(arg, str) and hasattr(self.inputs, arg):
                            arg = getattr(self.inputs, arg)
                        if isinstance(arg, list):
                            argtuple.append(arg[i])
                        else:
                            argtuple.append(arg)
                    filledtemplate = template
                    if argtuple:
                        try:
                            filledtemplate = template%tuple(argtuple)
                        except TypeError as e:
                            raise TypeError(e.message + ": Template %s failed to convert with args %s"%(template, str(tuple(argtuple))))
                    if self.inputs.hostname and self.inputs.username:
                        outfiles = self._map_to_local_cache(remote_glob(dict(username = self.inputs.username, hostname = self.inputs.hostname), filledtemplate))
                    else:
                        outfiles = glob.glob(filledtemplate)
                    if len(outfiles) == 0:
                        msg = 'Output key: %s Template: %s returned no files' % (key, filledtemplate)
                        if self.inputs.raise_on_empty:
                            raise IOError(msg)
                        else:
                            warn(msg)
                        outputs[key].insert(i, None)
                    else:
                        if self.inputs.sort_filelist:
                            outfiles.sort()
                        outputs[key].insert(i, list_to_filename(outfiles))
            if any([val is None for val in outputs[key]]):
                outputs[key] = []
            if len(outputs[key]) == 0:
                outputs[key] = None
            elif len(outputs[key]) == 1:
                outputs[key] = outputs[key][0]
        return outputs


## TODO
# in the exec part pandas doesn't like columns with dots in them
# perhaps force rename header on read, or find out how to make it work


class CSVFileInputSpec(TraitedSpec):                                                                                                 
    csv_filepath = traits.File(mandatory = True, desc = "path to input csv file")
    rename_header = traits.Dict(desc = """
    dictionary of the form

    headername : newheadername
    """)

class CSVFileOutputSpec(TraitedSpec):
    """
    should be list of Bunch objects
    
    """
    data_frame = traits.Any()

class CSVFile(BaseInterface):

    input_spec = CSVFileInputSpec
    output_spec = CSVFileOutputSpec

    def _run_interface(self, runtime):
        runtime.returncode = 0
        return runtime

    def __init__(self, **inputs):
        super(CSVFile, self).__init__(**inputs)

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['data_frame'] = self._data_frame
        return outputs

    def _run_interface(self, runtime):
        self.inputs.rename_header = self.inputs.rename_header or {}
        cwd = os.getcwd()
        df = pd \
                .read_csv(self.inputs.csv_filepath) \
                .rename(columns = self.inputs.rename_header) \
                .sort('onset')

        ## derive a duration column if it doesn't exist
        if (df.columns == "duration").sum() == 0:
            df['duration'] = df['onset'].diff().shift(-1)

        ## FIXME
        ## stuff the last value with the mean duration
        ## there isn't any empirical reason why this is
        ## a good idea
        df['duration'][-1:] = df['duration'].mean()
        self._data_frame = df
        runtime.returncode = 0
        return runtime


class DataSelectorInputSpec(TraitedSpec):                                                                                                 
    data_frame = traits.Any() # should be a pandas DataFrame
    condition_definition = traits.List()
    function_definition = traits.Any()
    condition_value_feeder = traits.Dict(traits.Str, value = {}, usedefault = True)

    # copied from DataSink
    def __setattr__(self, key, value):
        if key not in self.copyable_trait_names():
            if not isdefined(value):
                super(DataSelectorInputSpec, self).__setattr__(key, value)
            self.condition_value_feeder[key] = value
        else:
            if key in self.condition_value_feeder:
                self.condition_value_feeder[key] = value
            super(DataSelectorInputSpec, self).__setattr__(key, value)

    def __getattr__(self, key):
        if key not in self.condition_value_feeder:
            if len(self.condition_value_feeder) is 0:
                self.__setattr__(key, traits.Any())
            else:
                return None
        return self.condition_value_feeder[key]

class DataSelectorOutputSpec(TraitedSpec):
    """
    should be list of Bunch objects
    
    """
    ## data_frame = traits.Any()
    subject_info = traits.Any()

class DataSelector(BaseInterface):

    input_spec = DataSelectorInputSpec
    output_spec = DataSelectorOutputSpec

    def _run_interface(self, runtime):
        runtime.returncode = 0
        return runtime

    def __init__(self, **inputs):
        super(DataSelector, self).__init__(**inputs)

    def _list_outputs(self):
        outputs = self.output_spec().get()
        ## outputs['data_frame'] = self._data_frame
        outputs['subject_info'] = self._subject_info
        return outputs

    def _k2var(self, k):
        # return "col_%s" % k
        return k

    def _run_interface(self, runtime):
        ENDTIME_COLNAME = 'xx_post_onset_termination'
        cwd = os.getcwd()
        df = self.inputs.data_frame

        self._dfn = {}
        if self.inputs.function_definition:
            if type(self.inputs.function_definition) is list:
                for fn in self.inputs.function_definition:
                    self._dfn[fn.__name__] = fn
            else:
                self._dfn = self.inputs.function_definition

        df[ENDTIME_COLNAME] = df['onset'] + df['duration']
        for k in df.keys():
            if k.startswith("Unnamed"): continue
            varname = self._k2var(k)
            exec("%s = df['%s']" % (varname, k))

        dcond = {}
        dcount = {}
        row_sum = 0
        for conddef in self.inputs.condition_definition:
            if type(conddef) is tuple:
                condname, condfn = conddef
                idx_match = condfn(df)
            else:
                spl_conddef = conddef.split('=', 1)

                # assume the condition is specified directly by a derivation function
                if len(spl_conddef) == 1:
                    condname = evalstr = spl_conddef[0]
                else:
                    condname = spl_conddef[0].strip()
                    evalstr = spl_conddef[1].strip().format(**self.inputs.condition_value_feeder)
                
                if evalstr in self._dfn:
                    idx_match = self._dfn[evalstr](df)
                else:
                    idx_match = eval(evalstr)

            if condname == "remove!":
                df = df[~idx_match]
            else:
                dcond[condname] = df[idx_match]
                dcount[conddef] = dcond[condname].shape[0]
                row_sum += dcond[condname].shape[0]
            del idx_match

        dfonset = pd.concat(dcond.values())

        ## FIXME
        ## make this more flexible
        ## essentially we want to sort from slow to fast
        ## on certain given columns
        ## i.e. run -> trial -> onset
        ## because a similar onset may appear over different runs
        lsksort = []
        if "run_number" in df.keys():
            lsksort.append("run_number")
        if "trial_number" in df.keys():
            lsksort.append("trial_number")
        lsksort.append("onset")

        ## FIXME:
        ## the overlap check tolerance below should be configurable
        ## check for time overlap (some other event happens during another event)
        ## naive test of whether any onset begins before the previous row's end time
        dfonset_sorted = dfonset.sort(lsksort)
        idx_is_overlap = dfonset_sorted.index[(
            ## next event's onset time + tolerance < this event's end time
            ((dfonset_sorted['onset'][1:] + 1.2) < dfonset_sorted[ENDTIME_COLNAME][:-1])
            ## the check only applies when the
            ## next event's onset time must also > this event's onset time
            & (dfonset_sorted['onset'][:-1] < dfonset_sorted['onset'][1:])
            )]
        if len(idx_is_overlap):
            sidx_is_overlap = idx_is_overlap.to_series()
            sidx_is_overlap = sidx_is_overlap.append(sidx_is_overlap - 1)
            sidx_is_overlap.sort()
            ## get the accused rows and the rows preceding those
            print("""
            * * * WARNING: * * *

            There are overlaps in your event specification!

            \n%s
            """ % (dfonset_sorted.ix[sidx_is_overlap]))

        ## check for duplicate onsets
        idx_duplicated_onset = dfonset.duplicated('onset')
        if idx_duplicated_onset.sum() > 0:
            v_duplicated_onset = dfonset[idx_duplicated_onset]['onset']
            col_display = [k for k in dfonset.keys() if k != ENDTIME_COLNAME]
            raise Exception("""
                    Overlapping events found!

                    These onsets are duplicated in your event specification:

                    \n%s
            
            """ % (dfonset[dfonset['onset'].isin(v_duplicated_onset)][col_display]))

        ## check for over-specified model
        if row_sum > df.shape[0]:
            raise Exception("""
                    There are more events in your specification
                    than there are total rows in the data!
                    You specified : %s
                    Total rows    : %s

                    The conditions you specified:
                    =============================
                    %s
                    """ % (row_sum, df.shape[0], str("\n"+(" " * 20)).join(["(%s) %s"%(str(count).rjust(3), conddef) for conddef,count in dcount.items()])))

        ## self._data_frame = self.inputs.data_frame
        lsk = dcond.keys()
        self._subject_info = Bunch(
                conditions = lsk,
                onsets = [dcond[k]['onset'].tolist() for k in lsk],
                durations = [dcond[k]['duration'].tolist() for k in lsk],
                )

        runtime.returncode = 0
        return runtime


if __name__ == "__main__":
    

    from pandas import DataFrame as DF
    import sys


    if "--test-datagrabber" in sys.argv:
        print 'OK'

        sys.exit()

    ## this stuff should be moved to a unit test
    if "--test" in sys.argv:


        ## generate some random csv datastructure
        import faker, random, tempfile
        FK = faker.Faker()
        ncol = random.randint(1, 5)
        nrow = random.randint(100, 400)

        df = DF(dict([(key, np.random.rand(1, nrow)[0]) for key in [FK.username() for i in range(ncol-1)] + ["RT"]]))
        for k in df.keys():
            if k == "RT": continue
            if random.random() > 0.5:
                ## turn the column into a binary value
                df[k] = df[k].round()

        TR_duration = 0.5 * random.randint(2, 6)
        ## append a duration and onset
        df['duration'] = TR_duration
        df['onset'] = df.index * TR_duration

        csv_filepath = tempfile.mktemp(suffix = ".csv")
        df.to_csv(csv_filepath)

        csvf = pe.Node(name = "csvfile", interface = CSVFile())
        csvf.inputs.csv_filepath = csv_filepath
        csvf.inputs.rename_header = {
                'current_time': 'onset',
                
                }
        ## res = csvf.run()
        ds = pe.Node(name = "dataselector", interface = DataSelector())
        ds.inputs.condition_definition = [
                "run1 = onset >= 10",
                "run2 = (onset > 20) & (onset < 30)",
                
                ]

        wf = pe.Workflow(name="wf")
        wf.base_dir = "/tmp"
        wf.config['crashdump_dir'] = "/tmp/crashdump"
        wf.connect([
            (csvf, ds, [("data_frame", "data_frame")]),
            ])

        res = wf.run()

        os.unlink(csv_filepath)
    else:

        from pdloader import df

        def chose_no(df):
            return df['response'] != 1

        ds = DataSelector(
                data_frame = df,
                condition_definition = [
                "remove! = label.str.contains('INFO:wait_for_scanner')",
                "PROD_run_1 = (run_number == 1) & (evname == 'PRODUCT')",
                "chose_yes = (run_number == {run_number}) & (evname == 'CHOICE') & (response == 1)",
                ## "chose_no", # or "chose_no = chose_no" or "whatever = chose_no"
                ## ("chose_no", chose_no), # or "chose_no = chose_no" or "whatever = chose_no"
            ])

        ds.inputs.function_definition = [
                chose_no,
                ]
        ## ds.inputs.condition_value_feeder = {
        ##         'run_number': 1,
        ##         
        ##         }
        ds.inputs.run_number = 1
        res = ds.run()
        print res.outputs

    #except Exception, e:
    #    raise e



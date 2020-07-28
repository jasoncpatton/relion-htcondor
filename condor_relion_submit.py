#!/usr/bin/env python3

import logging
import argparse
import shlex
import sys
import os
import re

from subprocess import getoutput
from shutil import which
from pathlib import Path

import htcondor
import htcondor.dags


def get_executable_path(command):
    executable_path = which(command)
    if executable_path is None:
        logging.error(f'Unable to find {command} in PATH ({os.environ.get("PATH")})')
        sys.exit(2)
    return executable_path


def get_shared_libs(executable, known_deps=None):
    """
    For a given executable, find all the necessary shared libraries.
    """
    # Assume these are on the remote host:
    blacklist_xfer_libs = set(["libc", 'libpthread', 'linux-vdso', 'libdl', 'libstdc++', 'libm', 'libgomp', 'libgcc_s'])
    # Shell equivalent:
    # $ readelf -d $NAME | grep NEEDED
    # Example line of output:
    #  0x0000000000000001 (NEEDED)             Shared library: [libmpi_cxx.so.1]
    output = getoutput(f'readelf -d "{executable}"')
    direct_deps = []
    needed_re = re.compile(r"^\s*[0-9a-fx]+\s+\(NEEDED\)\s+Shared library:\s+\[(.+)\]")
    for line in output.splitlines():
        m = needed_re.match(line)
        if not m:
            continue
        lib_fullname = m.groups()[0]
        lib_short = lib_fullname.split(".", 1)[0]
        if lib_short not in blacklist_xfer_libs:
            direct_deps.append(lib_fullname)

    # Now resolve the direct deps to files
    # $ ldd $NAME
    # Example line of output:
    # libmpi_cxx.so.1 => /usr/lib64/openmpi/lib/libmpi_cxx.so.1 (0x00007f372a8b7000)
    ldd_re = re.compile(r"\s*([\S]+)\s+=>\s+(.*)\s+\([0-9a-fx]+\)")
    output = getoutput(f'ldd "{executable}"')
    transfer_files = set()
    for line in output.splitlines():
        m = ldd_re.match(line)
        if not m:
            continue
        lib, location = m.groups()
        if (lib in direct_deps) and ('/home/' in location):
            transfer_files.add(location)

    if known_deps and transfer_files.issubset(known_deps):
        return transfer_files

    indirect_deps = set()
    for entry in transfer_files:
        new_entries = get_shared_libs(entry, known_deps = transfer_files)
        indirect_deps.update(new_entries)
    transfer_files.update(indirect_deps)

    return transfer_files


def parse_star_file(fname):
    in_loop = False
    in_loop_header = False
    headers = []
    header_re = re.compile(r"(_\S+)")
    header_multi_re = re.compile(r"(_\S+)\s+#([0-9]+)")
    entries = []
    i = 0
    for line in open(fname, 'r'):
        i += 1
        line = line.strip()
        if not line:
            headers = []
            in_loop = False
            in_loop_header = False
            continue
        if line == "loop_":
            in_loop = True
            in_loop_header = True
            continue
        if not in_loop:
            continue
        if in_loop_header:
            if not line.startswith("_"):
                in_loop_header = False
                headers.sort()
            else:
                match = header_multi_re.match(line)
                if match:
                    name, idx = match.groups()
                    headers.append((idx, name))
                    continue
                match = header_re.match(line)
                if match:
                    name = line.rstrip()
                    headers.append((name,))
                    continue
        col = line.split()
        if len(col) < len(headers):
            logging.warning(f"Error in line {i} of {fname}: Found {len(col)} values, expected {len(headers)}")
            continue
        entries.append(dict(zip(headers, col)))
    return entries


def write_star_file(fname, entries):
    intro = """# RELION version 3.0.8

data_

loop_
"""
    outro = "\n"

    if len(entries) > 0:
        headers = list(entries[0].keys())
        headers.sort()
    else:
        logging.warning(f'No entries found to write to {fname}')
        return

    with open(fname, 'w') as f:
        f.write(intro)
        for header in headers:
            if len(header) > 1:
                f.write(f'{header[1]} #{header[0]}\n')
            else:
                f.write(f'{header[0]}\n')
        for entry in entries:
            entry_str = '\t'.join([entry[header] for header in headers])
            f.write(f'{entry_str}\n')
        f.write(outro)


def parse_command(command):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('command_path', nargs=1)
    parser.add_argument('--i', dest='input_starfile')
    parser.add_argument('--o', dest='output_dir')
    return parser.parse_known_args(shlex.split(command))


def fix_command(cmd_args, remainder):
    # Simplify command and I/O args for running in a condor job
    # ./command --i $(starfile_in) --o output/
    cmd = f"./{Path(cmd_args.command_path).name}"
    i =   f"--i $(starfile_in)"
    o =    "--o output/"

    if cmd[-4:] == '_mpi':
        logging.warning( 'HTCondor jobs may have difficulty with MPI versions of RELION executables.')
        logging.warning(f'This job is tasked with running {cmd}. Try setting the number of MPI procs to 1.')

    return f"{cmd} {i} {o} {' '.join(remainder)}"


def get_submit(submit_config):
    submit_config['transfer_input_files'] = ', '.join(submit_config['transfer_input_files'])
    if 'transfer_output_remaps' in submit_config:
        submit_config['transfer_output_remaps'] = '"{}"'.format('; '.join(["{} = {}".format(*entry) for entry in submit_config['transfer_output_remaps'].items()]))
    return htcondor.Submit(submit_config)


def submit_dag(cmd_name, args, work_dir, submit_config, dag_varlist):
    dag = htcondor.dags.DAG()
    submit_description = get_submit(submit_config)
   
    # Wrap the command for the DAG post script
    post_cmd = Path(sys.argv[0])
    post_args = [
        '--post',
        '--command', f"BEGIN_COMMAND {args.command} END_COMMAND",
        '--outfile', args.outfile,
        '--errfile', args.errfile
    ]
    post_script = htcondor.dags.Script(post_cmd, post_args)

    work_node = dag.layer(
        name = f"{cmd_name}_work",
        dir = work_dir,
        submit_description = submit_description,
        vars = dag_varlist
    )
    
    post_node = work_node.child_layer(
        name = f"{cmd_name}_post",
        noop = True,
        dir = Path.cwd(),
        post = post_script
    )

    dag_file = htcondor.dags.write_dag(dag, work_dir)
    sub = htcondor.Submit.from_dag(str(dag_file))
    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        result = sub.queue(txn)
        with open(args.errfile, "w") as fp:
            if isinstance(result, int):
                print(f"{dag_file} submitted in cluster {result}", file=fp)
            else:
                print(f"{dag_file} submitted in cluster {result.cluster()}", file=fp)


def fix_args():
    # Unwrap the command from DAG post script call
    if ("BEGIN_COMMAND" in sys.argv) and ("END_COMMAND" in sys.argv):
        i = sys.argv.index('BEGIN_COMMAND')
        j = sys.argv.index('END_COMMAND')
        sys.argv[i] = ' '.join(sys.argv[i+1:j])
        del sys.argv[i+1:j+1]


### Command-specific functions

def run_motioncorr_work(submit_config, cmd_args, work_dir):
    '''relion_run_motioncorr
    # `which relion_run_motioncorr` --i Import/job001/movies.star --o MotionCorr/job006/
    # --first_frame_sum 1 --last_frame_sum 0 --use_own --j 1 --bin_factor 1 --bfactor 150 --dose_per_frame 1.277 --preexposure 0
    # --patch_x 5 --patch_y 5 --gainref Movies/gain.mrc --gain_rot 0 --gain_flip 0 --dose_weighting --only_do_unfinished
    '''

    # Transfer gainref and load it from the scratch directory
    transfer_input_files = submit_config['transfer_input_files']
    args = shlex.split(submit_config['arguments'])
    gainref = args[args.index('--gainref') + 1]
    transfer_input_files.add(str(Path.cwd() / gainref))
    args[args.index('--gainref') + 1] = f'{Path(gainref).name}'

    # Add other input files
    transfer_input_files.add(str(work_dir / '$(starfile_in)'))
    transfer_input_files.add(str(Path.cwd() / '$(movie_file)'))

    # Set up outputs
    transfer_output_files = ['output/corrected_micrographs.star',
                                 'output/$(movie_basename).mrc',
                                 'output/$(movie_basename).star',
                                 'output/$(movie_basename)_shifts.eps']
    transfer_output_remaps = {
        'corrected_micrographs.star': '$(starfile_out)',
        '$(movie_basename).mrc':        '../Movies/$(movie_basename).mrc',
        '$(movie_basename).star':       '../Movies/$(movie_basename).star',
        '$(movie_basename)_shifts.eps': '../Movies/$(movie_basename)_shifts.eps',
    }
    (work_dir.parent / 'Movies').mkdir(parents=True, exist_ok=True)

    # Set up list of variables from the input star file
    dag_varlist = []
    entries = parse_star_file(cmd_args.input_starfile)
    for entry in entries:
        # Need to copy from movies.star:
        # _rlnMicrographMovieName
        col_names = list(entry.keys())
        col_names.sort()

        # Add the full paths to the dag vars for file transfer,
        # get the basename of the micrograph file,
        # and name the input and output starfiles
        col_data = entry.copy()
        movie_basename = Path(col_data[col_names[0]]).stem
        starfile_in = f'{movie_basename}_in.star'
        starfile_out = f'{movie_basename}_out.star'
        dag_vars = {
            'movie_file': col_data[col_names[0]],
            'movie_basename': movie_basename,
            'starfile_in': starfile_in,
            'starfile_out': starfile_out,
        }
        dag_varlist.append(dag_vars.copy())

        # Modify the entry, truncating paths
        entry[col_names[0]] = Path(entry[col_names[0]]).name

        # Write the input star file to the work dir
        write_star_file(work_dir / starfile_in, [entry])

    submit_config['arguments'] = ' '.join(args)
    submit_config['transfer_input_files'] = transfer_input_files
    submit_config['transfer_output_files'] = ', '.join(transfer_output_files)
    submit_config['transfer_output_remaps'] = transfer_output_remaps

    # Additional job requirements
    submit_config['request_memory'] = '3GB'

    return (submit_config, dag_varlist)


def run_motioncorr_post(cmd_args, work_dir):
    '''relion_run_ctffind
    Requires:
    1. Merge of individual output starfiles into entire output starfile corrected_micrographs.star
    2. logfile.pdf
    '''
    # Create a dummy logfile
    logfile_path = Path(cmd_args.output_dir) / 'logfile.pdf'
    logfile_path.touch()

    # Create corrected_micrographs.star
    entries_out = []
    for starfile in work_dir.glob('*_out.star'):
        entry = parse_star_file(starfile)[0]
        keys = list(entry.keys())
        keys.sort()

        mrc_path = Path(cmd_args.output_dir) / 'Movies' / Path(entry[keys[0]]).name
        if not mrc_path.exists():
            logging.warning(f'{mrc_path} from {starfile} does not exist\n')
            continue
        mrc_starfile_path = Path(cmd_args.output_dir) / 'Movies' / Path(entry[keys[1]]).name
        if not mrc_starfile_path.exists():
            logging.warning(f'{mrc_path} from {starfile} does not exist\n')
            continue

        entry[keys[0]] = str(mrc_path)
        entry[keys[1]] = str(mrc_starfile_path)
        entries_out.append(entry)

    # Write corrected_micrographs.star
    write_star_file(str(Path(cmd_args.output_dir) / 'corrected_micrographs.star'), entries_out)


def run_ctffind_work(submit_config, cmd_args, work_dir):
    '''relion_run_ctffind
    # `which relion_run_ctffind` --i MotionCorr/job002/corrected_micrographs.star --o CtfFind/job032/
    # --CS 2.7 --HT 300 --AmpCnst 0.1 --XMAG 10000 --DStep 1.4 --Box 512 --ResMin 30 --ResMax 5 --dFMin 5000 --dFMax 50000 --FStep 500 --dAst 100
    # --ctffind_exe ctffind --ctfWin -1 --is_ctffind4 --fast_search --only_do_unfinished
    Requires:
    1. Micrograph file $(mrc_file)
    2. Micrograph starfile $(mrc_starfile)
    3. Individual input starfile $(starfile_in) per entry in entire input starfile
    4. Rename of output starfile $(starfile_out)
    5. Rename and relocation of output CTF $(mrc_basename).ctf
    '''

    # Transfer ctffind and execute it from the scratch directory
    transfer_input_files = submit_config['transfer_input_files']
    args = shlex.split(submit_config['arguments'])
    ctffind_exe = get_executable_path(args[args.index('--ctffind_exe') + 1])
    transfer_input_files.add(ctffind_exe)
    transfer_input_files.update(get_shared_libs(ctffind_exe))
    args[args.index('--ctffind_exe') + 1] = f'./{Path(ctffind_exe).name}'

    # Add other input files
    transfer_input_files.add(str(work_dir / '$(starfile_in)'))
    transfer_input_files.add(str(Path.cwd() / '$(mrc_file)'))
    transfer_input_files.add(str(Path.cwd() / '$(mrc_starfile)'))

    # Set up outputs
    transfer_output_files = ['output/micrographs_ctf.star', 'output/$(mrc_basename).ctf']
    transfer_output_remaps = {
        'micrographs_ctf.star': '$(starfile_out)',
        '$(mrc_basename).ctf': '../Movies/$(mrc_basename).ctf',
    }
    (work_dir.parent / 'Movies').mkdir(parents=True, exist_ok=True)

    # Set up list of variables from the input star file
    dag_varlist = []
    entries = parse_star_file(cmd_args.input_starfile)
    for entry in entries:
        # Need to copy from corrected_micrographs.star:
        # _rlnMicrographName #1
        # _rlnMicrographMetadata #2
        col_names = list(entry.keys())
        col_names.sort()

        # Add the full paths to the dag vars for file transfer,
        # get the basename of the micrograph file,
        # and name the input and output starfiles
        col_data = entry.copy()
        mrc_basename = Path(col_data[col_names[0]]).stem
        starfile_in = f'{mrc_basename}_in.star'
        starfile_out = f'{mrc_basename}_out.star'
        dag_vars = {
            'mrc_file': col_data[col_names[0]],
            'mrc_starfile': col_data[col_names[1]],
            'mrc_basename': mrc_basename,
            'starfile_in': starfile_in,
            'starfile_out': starfile_out,
        }
        dag_varlist.append(dag_vars.copy())

        # Modify the entry, truncating paths
        entry[col_names[0]] = Path(entry[col_names[0]]).name
        entry[col_names[1]] = Path(entry[col_names[1]]).name

        # Write the input star file to the work dir
        write_star_file(work_dir / starfile_in, [entry])

    submit_config['arguments'] = ' '.join(args)
    submit_config['transfer_input_files'] = transfer_input_files
    submit_config['transfer_output_files'] = ', '.join(transfer_output_files)
    submit_config['transfer_output_remaps'] = transfer_output_remaps

    return (submit_config, dag_varlist)


def run_ctffind_post(cmd_args, work_dir):
    '''relion_run_ctffind
    Requires:
    1. Merge of individual output starfiles into entire output starfile micrographs_ctf.star
    2. logfile.pdf
    3. Symlink of input micrograph files inside Movies/ directory
    '''
    # Create a dummy logfile
    logfile_path = Path(cmd_args.output_dir) / 'logfile.pdf'
    logfile_path.touch()

    # Get dict matching mrc name to mrc path
    entries_in = parse_star_file(cmd_args.input_starfile)
    keys = list(entries_in[0].keys())
    keys.sort()
    mrc_paths = {}
    for entry in entries_in:
        mrc_path = entry[keys[0]]
        mrc_name = Path(entry[keys[0]]).name
        mrc_paths[mrc_name] = mrc_path

    # Create micrographs_ctf.star and symlink mrc files
    entries_out = []
    for starfile in work_dir.glob('*_out.star'):
        entry = parse_star_file(starfile)[0]
        keys = list(entry.keys())
        keys.sort()

        mrc_name = entry[keys[0]]
        if not (mrc_name) in mrc_paths:
            logging.warning(f'Did not find {mrc_name} from {starfile} in {cmd_args.input_starfile}\n')
            continue
        ctf_path = Path(cmd_args.output_dir) / 'Movies' / Path(entry[keys[1]].split(':')[0]).name
        mrc_ext = ':'.join(entry[keys[1]].split(':')[1:])
        if not ctf_path.exists():
            logging.warning(f'{ctf_path} from {starfile} does not exist\n')
            continue

        entry[keys[0]] = mrc_paths[mrc_name]
        entry[keys[1]] = f'{ctf_path}:{mrc_ext}'
        entries_out.append(entry)

        mrc_symlink_path = Path(cmd_args.output_dir) / 'Movies' / mrc_name
        if not mrc_symlink_path.exists():
            mrc_symlink_path.symlink_to(Path.cwd() / mrc_path)

    # Write micrographs_ctf.star
    write_star_file(str(Path(cmd_args.output_dir) / 'micrographs_ctf.star'), entries_out)

    
def main():
    #./condor_relion_submit.py [--post] --command="$COMMAND" --threads="$THREADS" --dedicated="$DEDICATED" --outfile="$OUTFILE" --errfile="$ERRFILE"
    fix_args()    
    parser = argparse.ArgumentParser()
    parser.add_argument('--command')
    parser.add_argument('--threads', type=int)
    parser.add_argument('--dedicated', type=int)
    parser.add_argument('--outfile')
    parser.add_argument('--errfile')
    parser.add_argument('--post', action='store_true')
    args = parser.parse_args()

    # set up logging
    logging.basicConfig(filename=args.outfile,
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # get command
    (cmd_args, remainder) = parse_command(args.command)
    cmd_args.command_path = cmd_args.command_path[0]
    cmd_name = Path(cmd_args.command_path).name
    logging.info(f'Setting up HTCondor for {cmd_name}')
    cmd_string = fix_command(cmd_args, remainder)

    # get work directory
    work_dir = Path.cwd() / cmd_args.output_dir / 'work'
    logging.info(f'Creating HTCondor working directory: {work_dir}')
    work_dir.mkdir(parents=True, exist_ok=True)

    if not args.post:
        
        # set up initial transfer_input_files
        transfer_input_files = set([cmd_args.command_path])
        transfer_input_files.update(get_shared_libs(cmd_args.command_path))

        # set up submit config and empty dag var list
        submit_config = {
            'executable': str(Path.cwd() / 'condor_relion_wrapper.sh'),
            'arguments': cmd_string,
            'initialdir': str(work_dir),
            'transfer_input_files': transfer_input_files,
            'transfer_output_files': 'output/',
            'request_cpus': args.dedicated,
            'request_memory': f'{2*args.dedicated}GB',
            'request_disk': '1GB',
            'output': 'run_$(ClusterId).$(ProcId).out',
            'error': 'run_$(ClusterId).$(ProcId).err',
            'log': 'condor.log',
            'requirements': '(HasChtcSoftware == true)',
            'should_transfer_files': 'YES',
        }
        dag_varlist = []

        # Modify submit config based the relion step
        if cmd_name == 'relion_run_ctffind':
            (submit_config, dag_varlist) = run_ctffind_work(submit_config, cmd_args, work_dir)
        elif cmd_name == 'relion_run_motioncorr':
            (submit_config, dag_varlist) = run_motioncorr_work(submit_config, cmd_args, work_dir)
        else:
            logging.error('Do not recognize relion command {cmd_name}')
            sys.exit(2)

        # Submit dag
        logging.info(f'Submitting {len(dag_varlist)} separate {cmd_name} jobs to HTCondor')
        submit_dag(cmd_name, args, work_dir, submit_config, dag_varlist)

    else:
        # run post (cleanup) function
        logging.info(f'Merging {cmd_name} output from HTCondor')
        if cmd_name == 'relion_run_ctffind':
            run_ctffind_post(cmd_args, work_dir)
        elif cmd_name == 'relion_run_motioncorr':
            run_motioncorr_post(cmd_args, work_dir)
        else:
            logging.error('Do not recognize relion command {cmd_name}')
            sys.exit(2)
            
        logging.info(f'Done')


if __name__ == '__main__':
    main()

 #! /usr/bin/python
import numpy as np
import yaml
import sys
from optparse import OptionParser

import roslib
import rospy

roslib.load_manifest('rosbag')
import rosbag

import h5py

def h5append(dset, arr):
    n_old_rows = dset.shape[0]
    n_new_rows = len(arr) + n_old_rows
    dset.resize(n_new_rows, axis=0)
    dset[n_old_rows:] = arr


def main():
    """ Usage
    First, start your simulation and log the data using rosbag:
    rosbag record /gazebo/link_states [other topics] -o file_prefix
    
    Many topics can be recorded, but this file only parses the "/gazebo_msgs/link_states".
    Each data record has the following structure per link changing over time

    pose.x
    pose.y
    pose.z
    pose.qx
    pose.qy
    pose.qz
    pose.qw
    twist.vx
    twist.vy
    twist.vz
    twist.wx
    twist.wy
    twist.wz

    """

    # Option parsing
    parser = OptionParser()
    parser.add_option("-i", "--input_file", dest="input_file",
                      help="input bag FILEs. Use wildcards for multiple files",
                      metavar="FILE", action="store")
    parser.add_option("-o", "--output_file", dest="output_file",
                      help="output FILE", metavar="FILE")

    (options, _) = parser.parse_args()

    # Input validation
    if options.input_file is None:
        rospy.logerr("The input bag has to be specified")
        sys.exit()
    if options.output_file is None:
        rospy.logerr("The output file has to be specified")
        sys.exit()

    # File operations
    f = h5py.File(options.output_file, "w")

    filename = options.input_file

    print "Opening bag %s" % filename        
    bag = rosbag.Bag(filename)
    bag_info = yaml.load(bag._get_yaml_info())

    # Set the dimension labels, units and description as groups
    f.create_group('x')
    f['x'].attrs.create('Units', 'meter')
    f['x'].attrs.create('Description', 'Position along x axis')

    f.create_group('y')
    f['y'].attrs.create('Units', 'meter')
    f['y'].attrs.create('Description', 'Position along y axis')

    f.create_group('z')
    f['z'].attrs.create('Units', 'meter')
    f['z'].attrs.create('Description', 'Position along z axis')

    f.create_group('qx')
    f['qx'].attrs.create('Units', 'none')
    f['qx'].attrs.create('Description', 'Quaternion x-like component')

    f.create_group('qy')
    f['qy'].attrs.create('Units', 'none')
    f['qy'].attrs.create('Description', 'Quaternion y-like component')

    f.create_group('qz')
    f['qz'].attrs.create('Units', 'none')
    f['qz'].attrs.create('Description', 'Quaternion z-like component')

    f.create_group('qw')
    f['qw'].attrs.create('Units', 'none')
    f['qw'].attrs.create('Description', 'Quaternion w-like component')

    f.create_group('vx')
    f['vx'].attrs.create('Units', 'meter')
    f['vx'].attrs.create('Description', 'Linear velocity along x axis')

    f.create_group('vy')
    f['vy'].attrs.create('Units', 'meter')
    f['vy'].attrs.create('Description', 'Linear velocity along y axis')

    f.create_group('vz')
    f['vz'].attrs.create('Units', 'meter')
    f['vz'].attrs.create('Description', 'Linear velocity along z axis')

    f.create_group('wx')
    f['wx'].attrs.create('Units', 'meter')
    f['wx'].attrs.create('Description', 'Angular velocity along x axis')

    f.create_group('wy')
    f['wy'].attrs.create('Units', 'meter')
    f['wy'].attrs.create('Description', 'Angular velocity along y axis')

    f.create_group('wz')
    f['wz'].attrs.create('Units', 'meter')
    f['wz'].attrs.create('Description', 'Angular velocity along z axis')

    # Get the size of the datasets Number of links and messages
    set_num_links = True
    num_links = 0

    data = {}
    dsets = {}

    for _, msg, _ in bag.read_messages(topics="/gazebo/link_states"):

        if set_num_links:
            num_links = len(msg.name)
            set_num_links = False

        names = getattr(msg, "name")
        poses = getattr(msg, "pose")
        twists = getattr(msg, "twist")

        # Iterate over links
        for b, name in enumerate(names):
            state_name = [poses[b].position.x, poses[b].position.y, poses[b].position.z,
                          poses[b].orientation.x, poses[b].orientation.y, poses[b].orientation.z, poses[b].orientation.w,
                          twists[b].linear.x, twists[b].linear.y, twists[b].linear.z,
                          twists[b].angular.x, twists[b].angular.y, twists[b].angular.z]

            state_tuple = tuple(state_name)

            if name not in data:
                data[name] = dict(dtype=np.float64, object=[state_tuple])
            else:
                data[name]['object'].append(state_name)

            arr = np.array(**data[name])

            if name not in dsets:
                dset = f.create_dataset(name, data=arr, maxshape=(None,13), compression='gzip', compression_opts=9)
                dsets[name] = dset
            else:
                h5append(dsets[name], arr)

            del arr
            data[name]['object'] = []

    f.close()

    print "==============================="
    print " Done!"
    print " Saved the HDF5 file with "
    print bag_info['messages']
    print " points for "
    print num_links
    print " bodies in a simulation of "
    print bag_info['duration']
    print " seconds."
    print "================================"


if __name__ == '__main__':
    main()

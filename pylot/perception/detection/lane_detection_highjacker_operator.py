import pickle

import erdos
from erdos import Message, ReadStream, Timestamp, WriteStream

from pylot.perception.messages import LanesMessage

class LaneDetectionHighjackerOperator(erdos.Operator):
    """An operator that outputs lane messages based on the values
    in `mlco_list`.
    """

    def __init__(self, camera_stream: erdos.ReadStream,
                 detected_lanes_stream: erdos.WriteStream, flags):
        self.frame_index = 0  # Initialize frame_index
        
        camera_stream.add_callback(self.on_camera_frame,
                                   [detected_lanes_stream])
        self._flags = flags
        self._logger = erdos.utils.setup_logging(self.config.name,
                                                 self.config.log_file_name)
        
        # Load the mlco_list from a pickled file.
        # f = open(self._flags.mlco_list_path, 'rb')
        # self.mlco_list = pickle.load(f)
        # f = close()
        with open(self._flags.mlco_list_path, 'rb') as f:
            self.mlco_list = pickle.load(f)
        print(str(mlco_list))
        # self.mlco_list_name = self._flags.mlco_list.name

    @staticmethod
    def connect(camera_stream: erdos.ReadStream):
        """Connects the operator to other streams.
        Args:
            camera_stream (:py:class:`erdos.ReadStream`): The stream on which
                camera frames are received.
        Returns:
            :py:class:`erdos.WriteStream`: Stream on which the operator sends
            :py:class:`~pylot.perception.messages.LanesMessage` messages.
        """
        detected_lanes_stream = erdos.WriteStream()
        return [detected_lanes_stream]

    @erdos.profile_method()
    def on_camera_frame(self, msg: erdos.Message,
                        detected_lanes_stream: erdos.WriteStream):
        """Invoked whenever a frame message is received on the stream.
        Args:
            msg: A :py:class:`~pylot.perception.messages.FrameMessage`.
            detected_lanes_stream (:py:class:`erdos.WriteStream`): Stream on
                which the operator sends
                :py:class:`~pylot.perception.messages.LanesMessage` messages.
        """
        self._logger.debug('@{}: {} received message'.format(
            msg.timestamp, self.config.name))
        assert msg.frame.encoding == 'BGR', 'Expects BGR frames'

        # # Optional: reformat the image data as an RGB numpy array.
        # image = cv2.resize(msg.frame.as_rgb_numpy_array(), (512, 256),
        #                    interpolation=cv2.INTER_LINEAR)
        # image = image / 127.5 - 1.0

        # Decode the MLCO individuals and write them to the
        # detected_lanes_stream.
        detected_lanes = self.decode_mlco()
        self._logger.debug('@{}: Detected {} lanes'.format(
            msg.timestamp, len(detected_lanes)))
        detected_lanes_stream.send(erdos.Message(msg.timestamp,
                                                 detected_lanes))

        self.frame_index += 1

    def decode_mlco(self):
        """Translates an element in the `mlco_list` to pylot.Lane format.
        Returns:
            :py:class:`d
        """
        decoded_lanes = []

        # Assumption: only a maximum of 2-lane road in considered.

        mlco_snapshot_list = self.mlco_list[self.frame_index]
        for i in range(2):
            lane = self.list_to_lane(mlco_snapshot_list[i])
            decoded_lanes.append(lane)

        return decoded_lanes

    @staticmethod
    def list_to_transform(side_markings_list):
        """
        Translates the lane markings information from a list to
        Transfrom datatype. Returns a list of transforms.
        """
        side_markings_transforms = []
        for marking in side_markings_list:
            marking_location = Location(marking[0], marking[1], marking[2])
            marking_rotation = Rotation(marking[3], marking[4], marking[5])
            marking_transform = Transform(
                location=marking_location, rotation=marking_rotation)
        side_markings_transforms.append(marking_transform)

        return side_markings_transforms

    @classmethod
    def list_to_lane(cls, lane_list):
        """
        Translates a list of lane-related parameters into Lane format.
        """
        lane_index = lane_list[0]
        left_markings_list = lane_list[1]
        right_markings_list = lane_list[2]

        # Translate the markings list into transforms.
        left_markings_transforms = cls.list_to_transform(left_markings_list)
        right_markings_transforms = cls.list_to_transform(right_markings_list)

        lane = Lane(
            lane_index,
            left_markings_transforms,
            right_markings_transforms)

        return lane

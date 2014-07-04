# -*- coding: utf-8 -*-

#
# Internal Partition structure:
#
#     +===============+     +----------------+     +=================+
#     | cell          |     | cell_label     |     | hierarchy       |
#     +===============+     +----------------+     +=================+
#  +--| cell_id       |--+  | cell_label_id  |     | hierarchy_id    |--+
#  |  | cell_labels   |  +->| cell_id        |     | hierarchy_value |  |
#  |  | partial       |     | hierarchy_id   |<-+  | hierarchy_level |  |
#  |  +---------------+     | label_id       |<-+  +-----------------+  |
#  |                        +----------------+  |                       |
#  |   +----------------+                       |  +-----------------+  |
#  |   | property       |  +----------------+   |  | label           |  |
#  |   +----------------+  | partition      |   |  +-----------------+  |
#  |   | property_id    |  +----------------+   +--| label_id        |  |
#  |   | property_key   |  | partition_id   |   +--| hierarchy_id    |<-+
#  |   | property_value |  | partition_hash |      | label_value     |
#  |   | created_date   |  | created_date   |      +-----------------+
#  |   +----------------+  +----------------+
#  |                                      +----------------+
#  |          +======================+    | edge_weight    |
#  |          | edge                 |    +----------------+
#  |          +======================+    | edge_weight_id |--+
#  |       +--| edge_id              |--->| edge_id        |  |
#  |       |  | other_partition_hash |    | weight_type    |  |
#  |       |  | other_partition_file |    | weight_note    |  |
#  |       |  +----------------------+    | proportional   |  |
#  |       |                              +----------------+  |
#  |       |                                                  |
#  |       |  +-----------------+     +--------------------+  |
#  |       |  | relation        |     | relation_weight    |  |
#  |       |  +-----------------+     +--------------------+  |
#  |       |  | relation_id     |--+  | relation_weight_id |  |
#  |       +->| edge_id         |  |  | edge_weight_id     |<-+
#  |          | other_cell_id   |  +->| relation_id        |
#  +--------->| cell_id         |     | weight_value       |
#             +-----------------+     +--------------------+
#

class Partition(object):
    pass

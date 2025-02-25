# RDM Content Provider

## Overview

This content provider is used to create a Jupyter notebook server from a GakuNin RDM project.

## Configuration

### Folder Mapping Configuration File: `paths.yaml`

When setting up an analysis environment from a GakuNin RDM project, the `paths.yaml` file can be used to specify which files should be copied into the image and which should be symbolically linked. This file explicitly lists file paths and directories and defines whether each path should be copied or symlinked.

The `paths.yaml` file serves a similar purpose to the `fstab` file in Linux, mapping folders in the GakuNin RDM project to appropriate locations within the image. It should be placed in the `.binder` or `binder` directory, and the image builder (`repo2docker`) will prioritize loading from these locations to automatically apply the necessary copy and symlink settings.

The syntax of `paths.yaml` is based on the `volumes` section in Docker Compose file specifications:
https://docs.docker.com/reference/compose-file/volumes/

An example `paths.yaml` is shown below:

```yaml
override: true
paths:
    - type: copy
      source: $default_storage_path/custom-home-dir
      target: .
    - type: link
      source: /googledrive/subdir
      target: ./external/googledrive
```

In the example above, `$default_storage_path/custom-home-dir` is copied to the root directory of the image, and `/googledrive/subdir` is symlinked to `./external/googledrive` within the image.

The `paths.yaml` file is written in YAML format as a dictionary. The top-level dictionary must include the following elements:

* `override`: Set to `true` to disable the default folder mapping (which copies the default storage content to the current directory). If omitted, it is treated as `false`.
* `paths`: A list defining how each file or folder should be handled. Each item is a dictionary specifying the behavior for a specific path.

#### Elements in the `paths` List

Each item in the `paths` list is a dictionary containing the following keys:

* `type`: Specifies the operation to apply to the folder. Must be either `copy` (copies files from the source) or `link` (creates a symbolic link).
* `source`: The path to the file/folder within the GakuNin RDM project. For example, to specify a folder named `testdir` in a Google Drive storage provider, use `/googledrive/testdir`. The variable `$default_storage_path` can be used to refer to the project’s default storage (note: the default storage is not necessarily `osfstorage`, depending on the institution).
* `target`: Specifies where the file/folder should be placed in the analysis environment. This must be a relative path from the output directory (the home directory when the environment starts). To explicitly indicate a relative path, only paths starting with `.` or `./` are allowed.

> Absolute paths are not allowed for `target`, to prevent the injection of unauthorized executables into the `repo2docker` environment.

If no `paths.yaml` is provided, the default behavior is as follows:

```yaml
paths:
    - type: copy
      source: $default_storage_path
      target: .
```

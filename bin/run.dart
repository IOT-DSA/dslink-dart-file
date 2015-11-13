import "dart:async";
import "dart:convert";
import "dart:io";

import "dart:typed_data";

import "package:dslink/dslink.dart";
import "package:dslink/nodes.dart";
import "package:crypto/crypto.dart";

LinkProvider link;

String hashString(String input) => CryptoUtils.bytesToHex((
    new SHA1()..add(UTF8.encode(input))
).close());

List<FileNode> fileNodes = [];

class FileNode extends SimpleNode {
  File file;
  StreamSubscription sub;

  FileNode(String path) : super(path);

  bool isBinary = false;

  @override
  onCreated() async {
    configs[r"$writable"] = "write";
    var filePath = attributes["@filePath"];

    if (filePath == null) {
      link.removeNode(path);
      return;
    }

    if (attributes["@fileBinary"] == true) {
      isBinary = true;
    }

    file = new File(filePath);
    configs[r"$type"] = "string";

    link.addNode("${path}/remove", REMOVE_ACTION);

    if (!fileNodes.contains(this)) {
      fileNodes.add(this);
    }
  }

  @override
  onSubscribe() {
    subs++;
    checkSubscriptions();
  }

  @override
  onUnsubscribe() {
    subs--;
    checkSubscriptions();
  }

  checkSubscriptions() async {
    if (subs < 0) {
      subs = 0;
    }

    if (subs == 0) {
      if (sub != null) {
        sub.cancel();
      }
      clearValue();
    } else {
      await loadValue();
      await subscribeToWatcher();
    }
  }

  subscribeToWatcher() async {
    if (sub != null) {
      sub.cancel();
    }

    try {
      sub = file.watch(events: FileSystemEvent.ALL).listen((FileSystemEvent event) async {
        await loadValue();

        if (event.type == FileSystemEvent.DELETE) {
          await subscribeToWatcher();
        }
      });
    } catch (e) {
      new Future.delayed(const Duration(seconds: 1), subscribeToWatcher);
    }
  }

  loadValue() async {
    try {
      if (!(await file.exists())) {
        updateValue(null);
        return;
      }

      if (isBinary) {
        Uint8List list;
        List<int> bytes = await file.readAsBytes();
        if (bytes is Uint8List) {
          list = bytes;
        } else {
          list = new Uint8List.fromList(bytes);
        }

        updateValue(list.buffer.asByteData());
      } else {
        try {
          updateValue(await file.readAsString());
        } on FormatException catch (_) {
          var bytes = await file.readAsBytes();
          updateValue(CryptoUtils.bytesToBase64(bytes));
        }
      }
    } catch (e) {}
  }

  int subs = 0;

  @override
  Map save() {
    var m = super.save();
    m.remove("?value");
    m.remove("remove");
    return m;
  }

  @override
  onRemoving() {
    if (sub != null) {
      sub.cancel();
    }

    fileNodes.remove(this);
  }

  bool needsToWrite = false;

  @override
  onSetValue(Object val) {
    if (isBinary) {
      if (val is String) {
        val = const Utf8Encoder().convert(val);
        data = val;
        needsToWrite = true;
        return true;
      }

      if (val is! ByteData) {
        return true;
      }
    } else {
      if (val is ByteData) {
        try {
          val = const Utf8Decoder().convert(
              (val as ByteData).buffer.asUint8List()
          );
          data = val;
          needsToWrite = true;
        } catch (e) {}
        return true;
      }

      if (val is! String) {
        val = val.toString();
        data = val;
        needsToWrite = true;
        return true;
      }
    }

    needsToWrite = true;
    data = val;
    return true;
  }

  dynamic data;
}

class GroupNode extends SimpleNode {
  GroupNode(String path) : super(path);

  @override
  onCreated() {
    link.addNode("${path}/addFile", ADD_FILE_ACTION);
    link.addNode("${path}/addGroup", ADD_GROUP_ACTION);
    link.addNode("${path}/remove", REMOVE_ACTION);
  }

  @override
  Map save() {
    var m = super.save();
    m.remove("addFile");
    m.remove("addGroup");
    m.remove("remove");
    return m;
  }
}

class AddFileNode extends SimpleNode {
  AddFileNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var rn = params["name"];
    var p = new Path(path);
    var fp = params["filePath"];
    var isBinary = params["binary"];
    if (rn == null || rn is! String || rn.isEmpty) {
      return {
        "success": false,
        "message": "Name not specified."
      };
    }

    if (isBinary is! bool) {
      isBinary = false;
    }

    if (fp == null || fp is! String || fp.isEmpty) {
      return {
        "success": false,
        "message": "File path not specified."
      };
    }

    var tname = "${p.parentPath}/${Uri.encodeComponent(rn)}";
    if (tname.startsWith("//")) tname.substring(1);
    var node = link.provider.getNode(tname);
    if (node != null && node.disconnected == null) {
      return {
        "success": false,
        "message": "Entity with name '${tname}' already exists."
      };
    }

    link.addNode(tname, {
      r"$is": "file",
      r"$name": rn,
      "@filePath": fp,
      "@fileBinary": isBinary
    });

    link.save();

    return {
      "success": true,
      "message": "Success."
    };
  }
}

class AddGroupNode extends SimpleNode {
  AddGroupNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var p = new Path(path);
    var pp = p.parentPath;
    var name = params["name"];

    if (name == null) {
      throw new Exception("name does not exist.");
    }

    var ep = "${pp}/${Uri.encodeComponent(name)}";
    if (ep.startsWith("//")) ep.substring(1);

    if (link.getNode(ep) != null) {
      return throw new Exception("Entity with name '${name}' already exists.");
    }

    link.addNode(ep, {
      r"$is": "group",
      r"$name": name,
      "addFile": ADD_FILE_ACTION
    });

    link.save();
  }
}

main(List<String> args) async {
  link = new LinkProvider(args, "File-", profiles: {
    "file": (String path) => new FileNode(path),
    "addFile": (String path) => new AddFileNode(path),
    "remove": (String path) {
      return new DeleteActionNode.forParent(path, link.provider as MutableNodeProvider, onDelete: () {
        link.save();
      });
    },
    "group": (String path) => new GroupNode(path),
    "addGroup": (String path) => new AddGroupNode(path)
  }, autoInitialize: false);

  link.init();

  SimpleNode addFileNode = link.addNode("/addFile", ADD_FILE_ACTION);
  addFileNode.serializable = false;

  SimpleNode addGroupNode = link.addNode("/addGroup", ADD_GROUP_ACTION);
  addGroupNode.serializable = false;

  var isLooping = false;

  timer = Scheduler.every(Interval.TWO_MILLISECONDS, () async {
    if (isLooping) {
      return;
    }
    isLooping = true;
    for (var x in fileNodes) {
      try {
        if (!x.needsToWrite) {
          continue;
        }

        if (x.data is ByteData) {
          x.data = (x.data as ByteData).buffer.asUint8List();
        }

        var val = x.data;
        x.needsToWrite = false;
        x.data = null;

        if (val is String) {
          await x.file.writeAsString(val);
        } else if (val is List) {
          await x.file.writeAsBytes(val);
        }
      } catch (e) {}
    }
    isLooping = false;
  });

  link.connect();
}

Timer timer;

final Map ADD_FILE_ACTION = {
  r"$is": "addFile",
  r"$name": "Add File",
  r"$params": [
    {
      "name": "name",
      "type": "string",
      "description": "File Name"
    },
    {
      "name": "filePath",
      "type": "string",
      "description": "File Path"
    },
    {
      "name": "binary",
      "type": "bool",
      "description": "Load as Binary"
    }
  ],
  r"$result": "values",
  r"$invokable": "write",
  r"$columns": [
    {
      "name": "success",
      "type": "bool"
    },
    {
      "name": "message",
      "type": "string"
    }
  ]
};

final Map ADD_GROUP_ACTION = {
  r"$is": "addGroup",
  r"$name": "Add Group",
  r"$params": [
    {
      "name": "name",
      "type": "string",
      "description": "File Name"
    }
  ],
  r"$result": "values",
  r"$invokable": "write"
};

final Map REMOVE_ACTION = {
  r"$name": "Remove",
  r"$is": "remove",
  r"$invokable": "write"
};

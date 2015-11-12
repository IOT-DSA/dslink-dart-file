import "dart:async";
import "dart:convert";
import "dart:io";

import "package:dslink/dslink.dart";
import "package:dslink/nodes.dart";
import "package:crypto/crypto.dart";

LinkProvider link;

String hashString(String input) => CryptoUtils.bytesToHex((
    new SHA1()..add(UTF8.encode(input))
).close());

class FileNode extends SimpleNode {
  File file;
  StreamSubscription sub;

  FileNode(String path) : super(path);

  @override
  onCreated() async {
    var filePath = attributes["@filePath"];

    if (filePath == null) {
      link.removeNode(path);
      return;
    }

    file = new File(filePath);
    configs[r"$type"] = "string";

    link.addNode("${path}/remove", {
      r"$name": "Remove",
      r"$is": "remove",
      r"$invokable": "write"
    });
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
      updateValue(await file.readAsString());
      sub = file.watch(events: FileSystemEvent.ALL).listen((FileSystemEvent event) async {
        if (event.type == FileSystemEvent.MODIFY || event.type == FileSystemEvent.CREATE) {
          updateValue(await file.readAsString());
        }
      });
    }
  }

  int subs = 0;

  @override
  Map save() {
    var m = super.save();
    m.remove("?value");
    m.remove("Remove");
    return m;
  }

  @override
  onRemoving() {
    if (sub != null) {
      sub.cancel();
    }
  }
}

main(List<String> args) async {
  link = new LinkProvider(args, "File-", profiles: {
    "file": (String path) => new FileNode(path),
    "addFile": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
      var rn = params["name"];
      var fp = params["filePath"];
      if (rn == null || rn is! String || rn.isEmpty) {
        return {
          "success": false,
          "message": "Name not specified."
        };
      }

      if (fp == null || fp is! String || fp.isEmpty) {
        return {
          "success": false,
          "message": "File path not specified."
        };
      }

      var tname = "/${hashString(rn)}";
      var node = link.provider.getNode(tname);
      if (node != null && node.disconnected == null) {
        return {
          "success": false,
          "message": "File with name '${tname}' already exists."
        };
      }

      link.addNode(tname, {
        r"$is": "file",
        r"$name": rn,
        "@filePath": fp
      });

      link.save();

      return {
        "success": true,
        "message": "Success."
      };
    }),
    "remove": (String path) =>
      new DeleteActionNode.forParent(path, link.provider as MutableNodeProvider)
  }, autoInitialize: false);

  link.init();

  SimpleNode addFileNode = link.addNode("/addFile", {
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
  });

  addFileNode.serializable = false;

  link.connect();
}

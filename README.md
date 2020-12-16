# README #


## Overview [&GreaterGreater;](#-getting-started-) ##

This connector implements a batch API for manipulating the Harmony API using
a file request/response model.
It is packaged as a Harmony connector, suitable
for use as a custom _Other Folder_ for portal users, allowing users to upload
batch request files and download corresponding result files.
It is also packaged as a command line utility in the form of an executable
java jar.

In both packages, the utility uses the REST API and must be configured with
a server url, username and password.
The connector stores this information securely in the connection profile.
The command line utility maintains a set of connection profiles in the
`$HOME/.cic/profiles` file.

Request files maybe either in YAML/JSON format or in CSV format. The YAML/JSON
format consists of a collection of requests based on the native Harmony
[REST API](https://developer.cleo.com/api/getting-started/overview.html) with
extensions to indicate the operation (`add`, `update`, `list`, `delete`, `run`).

There are five separate CSV templates, one for each of the following object types:

* Authenticators (user groups)
* Users
* AS2 connections
* SFTP connections
* FTP connections

The CSV templates use specific column headings to map to object properties using
a simpified, flattened approach compared with the full JSON object schema.
The CSV formats can be easier to work with for bulk imports (`add`s), but for full access
to all object properties or support for `list`, `delete`, `update` or to `run` actions, the YAML/JSON format is required.

## [&LessLess;](#overview-) Getting Started [&GreaterGreater;](#-password-generation-) ##

### Harmony Connector [&gt;](#-command-line-)

To use the Harmony Connector you must install the connector package, restart Harmony, and then configure the connector for use.

1. Obtain the connector package `connector-batchapi-0.9-RC7-distribution.zip`.
2. Install the connector in your Harmony:<br/>&bull; `Harmonyc -i connector-batchapi-0.9-RC7-distribution.zip`.
3. Restart Harmony.
4. Log back into the Harmony admin. From the Hosts tab add a new host:<br/>&bull; select `Connections`&rarr;`Generic`&rarr;`Generic BatchAPI`<br/>&bull; right-click `Clone and Activate`<br/>&bull; Click `Ok` and `Done`.
5. Setup your new `BatchAPI` host:<br/>&bull; On the `BatchAPI` tab set the Working Directory and create a profile in the Profiles table with a Url (e.g. `https://localhost:6080`), User and Password. The Working Directory is where result files are stored, and in a clustered setup must be shared between the Harmony servers in the cluster.
6. Test your new connection:<br/>&bull; In the `send` action use the single command `PUT test.yaml`<br/>&bull; create a request file `test.yaml` as shown [below](#getting-started-test-file) in your outbox directory.<br/>&bull; Run the action. You should see a `test.results.yaml` file in your working directory.


### [&lt;](#harmony-connector-) Command Line [&gt;](#-getting-started-test-file-)

The command line package does not require installation, but simply installation
of the prerequisites (Java 8&mdash;openjdk8 is fine on [Windows](https://access.redhat.com/documentation/en-us/openjdk/8/html/openjdk_8_for_windows_getting_started_guide/getting_started_with_openjdk_for_windows) or [Linux](https://openjdk.java.net/install/) or [Mac OS](https://github.com/AdoptOpenJDK/homebrew-openjdk)) and the executable jar
`connector-batchapi-0.9-RC7-commandline.jar`.

If Java and the command line jar are properly installed, the following will work:

```
$ java -jar connector-batchapi-0.9-RC7-commandline.jar --help
usage: com.cleo.labs.connector.batchapi.processor.Main
    --help
    --url <URL>                  VersaLex url
 -k,--insecure                   Disable https security checks
 -u,--username <USERNAME>        Username
 -p,--password <PASSWORD>        Password
 -i,--input <FILE>               input file YAML, JSON or CSV
    --generate-pass              Generate Passwords for users
    --export-pass <PASSWORD>     Password to encrypt generated passwords
    --operation <OPERATION>      default operation: list, add, update, delete or preview
    --output-format <FORMAT>     output format: yaml (default), json, or csv
    --output-template <TEMPLATE> template for formatting csv output
    --log <FILE>                 log to file when using output-template
    --include-defaults           include all default values when listing connections
    --template <TEMPLATE>        load CSV file using provided template
    --profile <PROFILE>          Connection profile to use
    --save                       Save/update profile
    --remove                     Remove profile
    --trace-requests             dump requests to stderr as a debugging aid
```

You can provide all the connection parameters (`url`, `username` and `password`) on the command line, but this is both inconvenient and insecure. Instead it is preferable to setup a profile for each Harmony server you need to work with.

To create a profile, use:

```
java -jar connector-batchapi-0.9-RC7-commandline.jar --url https://192.168.7.22 -u administrator -p Admin --save
```

This will create a default profile in `$HOME/.cic/profiles` that looks like:

```
---
default:
  url: "http://192.168.7.22"
  insecure: false
  username: "administrator"
  password: "Admin"
  exportPassword: null
```

Add the `--profile name` option to select a profile name other than `default`. Using named profiles you can save as many profiles as you need. You can also edit the `profiles` file directly, taking care to preserve its simple YAML format. In fact, since using passwords in command lines is insecure, it is recommended to edit the passwords manually in `profiles`. If you use the command line to create the profiles initially, it is better to use dummy passwords for subsequent replacement through manual edits.

When running the utility, the `default` profile will be loaded by default, unless an alternate profile is specified with `--profile name`. Any additional connection options supersede the profile values.

To verify your profile, process a simple [test file](#getting-started-test-file) with the `-i` option:

```
java -jar connector-batchapi-0.9-RC7-commandline.jar -i test.yaml
```

The results will be written to the standard output.

The special input filename `-` represents the standard input (use `-i ./-` if you really have a file named `-`). This can be used for a kind of shorthand query syntax like

```
java -jar connector-batchapi-0.9-RC7-commandline.jar -i - <<< '{"operation":"list","username":"bob"}'
```

or even more compactly

```
java -jar connector-batchapi-0.9-RC7-commandline.jar --operation list -i - <<< 'username: bob'
```

You may use `-i` multiple times to supply a sequence of input files to be processed. Keep in mind that while a single input file can only be in one of the six supported formats (YAML/JSON or CSV for one of the five CSV object types), you can mix and match input file formats when using multiple  `-i`.

If any command line arguments remain after processing options, they are used as the text of the input in place of any `-i` or `--input` options. So the most compact form of the previous request is

```
java -jar connector-batchapi-0.9-RC7-commandline.jar --operation list 'username: bob'
```

### [&lt;](#-command-line-) Getting Started Test File [&GreaterGreater;](#-password-generation-)

Use this test file, edited as instructed, to verify your installation as described above. The expected test result file follows.

#### test.yaml ####

Use your own username or some other known username.

```
---
operation: list
type: user
username: you
```

#### test.results.yaml ####

```
---
- result:
    status: success
    message: found user you
  id: PMHTT7QjTg6EMyoxIQ15DA
  username: you
  email: you@yours.com
  authenticator: Your User Group
```

The result attributes will vary depending on the user details recorded. Note that default values for a user are suppressed from the results file.

## [&LessLess;](#-getting-started-) Password Generation [&GreaterGreater;](#-configuration-reference-) ##

When creating users, an initial value for the password must be provided (whether this initial password must be reset when the user logs in for the first time is controlled by the `accept.security.passwordRules.requirePasswordResetBeforeFirstUse` property of the authenticator, identified by the `alias` in the user creation request&mdash;see the API documentation for [Authenticators (Native User)](https://developer.cleo.com/api/api-reference/post-authenticators-native-user.html)).

The batch API utility includes an option for generating random passwords instead of having them supplied in the input file. Generated passwords have the following structure:

* 5 randomly selected upper case letters
* 1 randomly selected separator (from a set of 8 possible separators)
* 5 randomly selected digits
* 1 randomly selected separator
* 5 randomly selected lower case letters
* 1 randomly selected separator
* 5 randomly selected digits

An example generated password might be `FEWSH_77121|denco+13057`. This format ensures that most length and complexity requirements can be met, while also providing over 89 bits of entropy.

Passwords are included in the result record for added users. Additionally a result block summarizing added users and their passwords is appended to the results file:

```
- result:
    status: success
    message: generated passwords
    passwords:
    - authenticator: Users
      username: testUser
      email: testUser@test.com
      password: FEWSH_77121|denco+13057
    - authenticator: Users
      username: testUser2
      email: testUser2@test.com
      password: LJBXI_99080-orpug-12738
```

The passwords can then be communicated to the users out of band. For an additional layer of security in handling these credentials, the generated passwords may be encrypted by an _export password_, in which case the passwords are encrypted and encoded in base64 in the results file:

```
- result:
    status: success
    message: generated passwords
    passwords:
    - authenticator: Users
      username: testUser
      email: testUser@test.com
      password: U2FsdGVkX18KWfsFAdh3rQzFNE6d5noCbBd3cqiJu1Yw8oUgvPBCXomRne+ZqbAl
    - authenticator: Users
      username: testUser2
      email: testUser2@test.com
      password: U2FsdGVkX18yY07hCqwg+xn3st+KwKDqFr3BWRYE9NulzBirPgRHK4TFE4XENcc+
```

The encryption format is suitable for decryption using [openssl](https://wiki.openssl.org/index.php/Binaries) as follows:

```
$ openssl aes-256-cbc -d -a <<< "U2FsdGVkX18KWfsFAdh3rQzFNE6d5noCbBd3cqiJu1Yw8oUgvPBCXomRne+ZqbAl"
enter aes-256-cbc decryption password:
LJBXI_99080-orpug-12738
```

> Note that when CSV output format is selected (see [Formatting Results](#-formatting-results)) the password in the result record in `${data.accept.password}` can be mapped into the desired CSV output. The `generated passwords` result block is omitted for CSV output. `${data.accept.password}` is still encrypted by an _export password_ as described.

## [&LessLess;](#-password-generation-) Configuration Reference [&GreaterGreater;](#-request-processing-) ##

This table describes the options that control the Batch API utility, both in its command line and Harmony connector packagings. Note that the profile management options are available for the command line only&mdash;in Harmony you create separate connections of type `BatchAPI` to achive the same effect.

Command Line                    | Connector         | Description
--------------------------------|-------------------|------------
--url <URL>                     | Profile&rarr;Url  | The Harmony URL, e.g. `https://localhost:6080`
-u, --username &lt;USERNAME&gt; | Profile&rarr;User | The user authorized to use the Harmony API
-p, --password &lt;PASSWORD&gt; | Profile&rarr;Password| The user's password
-k, --insecure                  | Profile&rarr;Ignore TLS Checks | Select to bypass TLS hostname and trusted issuer checks
-i, --input &lt;FILE&gt;        | `PUT` file        | input file YAML, JSON or CSV
--generate-pass                 | Generate Password | Select to enable password generation for created users
--export-pass &lt;PASSWORD&gt;  | Export Password   | Password used to encrypt generated passwords in the results file
--operation &lt;OPERATION&gt;   | Default Operation | The default operation for entries lacking an explicit "operation"
--output-format &lt;FORMAT&gt;  | Output Format     | Output format: yaml (default), json, or csv
--output-template&nbsp;&lt;TEMPLATE&gt; | Output Template | Template for formatting csv output (required with csv)
--log&nbsp;&lt;FILE&gt;         | &nbdp;            | Also log YAML output to file when using output-template
--profile &lt;PROFILE&gt;       | &nbsp;            | The named profile to load instead of "default"
--include-defaults              | &nbsp;            | Include all default values when listing connections
--template &lt;TEMPLATE&gt;     | Template          | load CSV file using provided template
--save                          | &nbsp;            | Select to create/update named profile (or "default")
--remove                        | &nbsp;            | Select to remove named profile (or "default")


## [&LessLess;](#-configuration-reference-) Request Processing [&GreaterGreater;](#-multiple-profiles-) ##

### Requests [&gt;](#-results-)

Regardless of the input format (YAML/JSON or CSV), the input file is processed as a sequence of requests.
Each request has an _operation_ and an _object type_ to operate on.
Operations are typically performed on single objects referenced by the object name.
The underlying API uses `alias` to represent the object name, except for users who are identified by `username`&mdash;
the batch utility uses a type-specific _Identifier_ to name objects of different types.
Operations supporting sets of objects (list, update, delete and run) may specify a filter string in place of a specific object name&mdash;the filter expressions are passed directly to the underlying API, so make sure to use the appropriate `alias` or `username` attribute in filters (or use `$$name$$` and the utility will substitute the correct token based on the object type).

Object Type | Description | Identifier | Meta Type
------------|-------------|------------|-----
Authenticator | A container for users that defines many properties, including folder structure and security properties (see the [API reference](https://developer.cleo.com/api/api-reference/post-authenticators-native-user.html)) | `authenticator` | `authenticator`
User        | A user who logs in to Harmony using FTP, SFTP, or through the https Portal | `username` | `user`
Connection  | A connection to a server over FTP, SFTP, or AS2 | `connection` | `connection`
Action      | An action that runs under one of the other objects | `action` | `action`

Each object type is managed with its own endpoint in the underlying Harmony API, so the batch utility must be able to determine which endpoint to use for a given request. To do this it uses a combination of the object name and object type, depending on the request operation.

Each object type corresponds to a _meta type_ in the API (`meta.type`). Objects of type `authenticator` and `connection` also have a specific `type`:

Object Type   | Meta Type       | Type            | Description
--------------|-----------------|-----------------|------------
Authenticator | `authenticator` | `nativeUser`    | Users defined natively in the Harmony user table
&nbsp;        | &nbsp;          | `systemLdap`    | Users defined in the System LDAP directory
&nbsp;        | &nbsp;          | `authConnector` | Users defined through an authentication connector
Connection    | `connection`    | `as2`           | A connection to an AS2 partner
&nbsp;        | &nbsp;          | `sftp`          | A connection to an SFTP server
&nbsp;        | &nbsp;          | `ftp`           | A connection to an FTP server
&nbsp;        | &nbsp;          | _many others_   | See [here](https://developer.cleo.com/api/api-reference/resource-connections.html) for a list of many other supported connection types
User          | `user`          | `user`          | Users don't have a type, only a meta type
Action        | `action`        | `Commands`      | Actions comprised of "commands"
&nbsp;        | &nbsp;          | `JavaScript`    | Actions comprised of JavaScript "statements"

The batch utility uses a meta-type-specific tag for the object name, e.g. `username: name` for users, `connection: name` for connections, `authenticator: name` for authenticators and `action: action` for actions (action names are not unique, but are scoped to the parent object&mdash;see [below](#running-actions) for more details). For operations involving existing objects (i.e. anything other than `add`), the specific type does not need to be provided. For `add` operations, both the name of the new object and its specific type are required. `add` and `update` operations must also supply an _Entity Body_, the details of the object to be created or updated. For `list`, `delete` and `run` operations the entity body is ignored.

Operation | Description               | Filter Supported | Entity Body
----------|---------------------------|:----------------:|:-----------:
list      | List existing object(s)   | &check;          | &nbsp;
add       | Create a new object       | &nbsp;           | &check;
update    | Update an existing object | &check;          | &check;
delete    | Delete existing object(s) | &check;          | &nbsp;
run       | Run existing action(s)    | &check;          | &nbsp;
preview   | Template preview          | &ctdot;          | &ctdot;

The default operation is `add`, unless this is overridden with the `--operation <OPERATION>` argument for the command line utility. In any case, if an operation other than `preview` is specified in a request it is honored over the default (see [Testing your template](#testing-your-template)).

The `list`, `update`, `delete` and `run` operations may be applied to sets of objects identified by a [filter](https://developer.cleo.com/api/getting-started/overview.html#filter).

For example, to query for all SFTP connections, use:

```
---
- operation: list
  type: connection
  filter: type eq "sftp"
```

Again, remember to use `alias` in filter expressions if needed for a `connection`, `authenticator` or `action` filter, and use `username` for a `user` filter. You may also use the `$$name$$` token and the utility will supply `alias` or `username` as appropriate based on context. The following three requests are equivalent:

```
---
- operation: list
  type: connection
  filter: alias eq "mysftp"
- operation: list
  type: connection
  filter: $$name$$ eq "mysftp"
- operation: list
  connection: "mysftp"
```

If you omit `type` from a request, the utility will attempt to locate objects of any type matching the filter expression. In this case you *must* use the `$$name$$` token in place of `alias` and `username` (if you are querying based on the object name) so the proper subsitution can be made while searching the different types.

```
---
operation: list
filter: $$name$$ sw "d"
```

A blank `filter` matches everything, so:

```
---
operation: list
type: connection
filter: ""
```

will list all connections and:

```
---
operation: list
filter: ""
```

will list all objects (users, authenticators, and connections) in the configuration.

### [&lt;](#requests-) Results [&gt;](#-action-handling-)

Each request produces one or more results.
The results are formatted into a YAML collection where each entry has a `result` object:

```
---
- result:
    status: success or error
    message: description of the success or error
    optional additional result information...
  additional object information...
```

#### `list` operations

Single object requests (without `filter`) generate results whose _additional object information_ describes the object found:

```
---
- result:
    status: success
    message: found user edie
  id: 9ybMFuRpRjqRVJmE5HE5aQ
  username: edie
  email: edie@cleo.demo
  authenticator: Users
```

`filter` requests generate one result per object found with the count "m of n" in the message, also filling in _additional object information_:

```
- result:
    status: success
    message: found connection loopback sftp (1 of 2)
  id: JOe0WtAxTFS7q3fd0mdg2Q
  type: sftp
  connect:
    host: mysftp.cleo.demo
    port: 10022
    username: user1
  outgoing:
    storage:
      outbox: outbox/
    partnerPackaging: false
  incoming:
    storage:
      inbox: inbox/
    partnerPackaging: false
  connection: mysftp1
- result:
    status: success
    message: found connection vagrant sftp (2 of 2)
  id: WFZUCRdQSp-eVXuT-C7b0w
  type: sftp
  connect:
    host: mysftp.cleo.demo
    port: 22
    username: user2
  outgoing:
    storage:
      outbox: outbox/
    partnerPackaging: false
  incoming:
    storage:
      inbox: inbox/
    partnerPackaging: false
  connection: mysftp2
```

`list` operations for authenticators implicitly include all the user objects grouped under that authenticator, resulting in one additional result for each user found. These users are identified with result messages like "found authenticator Users: user m of n".

`list` operations, with or without `filter`, generate an appropriate error result if no matching object(s) is/are found.

#### `add` operations

Add requests generate results whose _additional object information_ describes the object created.

```
---
- result:
    status: success
    message: created test
  id: f9dz4JRZQcOpHbm-OvCwig
  username: test
  email: test@cleo.demo
  authenticator: Users
```

'add' operations generate an appropriate error result if an object is not created. If the batch utility does not encounter an error preparing the request, the error message is generated by the Harmony API itself.

#### `update` operations

Successful updates produce two separate results.
The first result, identified with a message like "updating user alice", includes a representation of the object before any updates were applied, exactly as if it had been produced by a `list` operation.
The second result, identified with a message like "user alice updated", includes a representation of the updated object.

Note that unlike `list` operations, `update` operations on authenticators do not affect users, so the additional results produced by `list` for these nested users are not included for `update`.

Bulk update requests may be applied to sets of objects using a `filter` in the request instead of naming a specific object. Any attributes provided in the request are merged/overlaid with the existing object attributes. The reported results appear in before/after pairs as described above.

> An `update` operation comprises two essential parts: the fields that identify the objects to be updated, and the fields that are meant to be updated. While internally the API operates on an `id`, the batch utility performs operations based on names&mdash;`username`, `authenticator`, `connection`, or `action` (`username` or `alias` internally, or `$$name$$` in a `filter`). If an update to a name is desired&mdash;renaming an object, or "moving" a user from one authenticator to another&mdash;the new name is indicated in the request in an `update` field:
> 
> ```
> ---
> operation: update
> username: bob
> authenticator: Users
> update:
>   username: robert
>   authenticator: NoNicknameUsers
> ```
> 
> This request will rename `bob` to `robert` and move the user from `Users` to `NoNicknameUsers`, including any actions that might be attached to `bob` (internally this requires `bob` to be deleted from `Users` and `robert` to be added to `NoNicknameUsers`).

#### `delete` operations

Results for `delete` requests are very similar to those for `list` requests, except that the objects listed are deleted with a result message like "deleted user alice". The results can be replayed as `add` requests to restore the deleted objects.

> Note that the command line batch utility is currently unable to list passwords, whether password hashes for users or encrypted passwords for connections, due to limitations of the underlying Harmony REST API. So while the `delete` results can be replayed as `add` requests, new passwords will have to be generated or the old passwords will need to be added from a another source. The Harmony connector version of the batch utility uses an additional API to export and import passwords.

Bulk delete requests may be applied to sets of objects using a `filter` in the request instead of naming a specific object. One result it reported for each object deleted, with a result message like "deleted user alice (m of n)".

### [&lt;](#-results-) Action Handling [&gt;](#-certificate-handling-)

In the native Harmony API, actions are a separate resource type, linked to connections, authenticators, and users through `_links`. The batch utility simplifies this processing by treating the set of actions for an object as a separate object nested within the parent object itself:

```
- username: testUser
  email: testUser@test.com
  authenticator: Users
  home:
    dir:
      override: local/root/run/
  actions:
    connectTest:
      action: connectTest
      commands:
      - GET -DEL *
      - LCOPY -REC %inbox% %inbox%/in
    other:
      action: other
      commands:
      - # other commands here
```

In the embedded `actions` object, each action is represented as a sub-object whose attribute name is the same as the action's `action` name (if the attribute name and `action` name disagree, the `action` name is used). A `list` operation for any object will render any linked actions as an embedded `actions` property as illustrated above.

On update, actions in the request are matched up against existing actions on the object by `action` name:

* any request actions not appearing in the existing object are added
* any request actions matching `action` name with existing actions are updated
* any existing actions with no matching `action` name in the request are left unchanged

In order to delete an existing action, create an action with the matching `action` name in the request, adding the property `operation: delete`:

```
- username: testUser
  email: testUser@test.com
  authenticator: Users
  home:
    dir:
      override: local/root/run/
  actions:
    other:
      alias: other
      opreation: delete
      commands:
      - # other commands here
```

You can also delete actions directly&mdash;see [Managing Actions](#managing-actions) below.

#### Running actions

You can run actions using the `run` operation. Use the `username` or `connection` properties in the request to identify the object whose action you want to run, and the `action` property to specify which of the user's or connection's actions to run:

```
- operation: run
  username: testUser
  action: other
```

or

```
- operation: run
  connection: mysftp
  action: dir
```

which will respond with the result:

```
- result:
    status: success
    message: ran action dir
  output:
    status: completed
    result: success
    messages:
    - "2020/09/24 17:46:57   Run: type=\"API\""
    - "2020/09/24 17:46:57   Command: \"DIR *\" type=\"SSH FTP\" line=1 threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:57      Detail: \"Connecting to ssh://127.0.0.1:22...\" threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:57      Detail: \"Server ID: SSH-2.0-OpenSSH_6.6.1p1 Ubuntu-2ubuntu2.13\" level=1 threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:57      Detail: \"RemotePort: 22\" level=1 threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:57      Detail: \"Authentication complete\" level=1 threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:58      Detail: \"Getting host file directory()...\" level=1 threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:58      SSH FTP: \"ls()\""
    - "2020/09/24 17:46:58      Detail: \"2 file(s) found\" level=1 threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:58      Detail: \"-rw-rw-r-- 1000   1000              915 Sep 16 15:36 SERVER.req\" threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:58      Detail: \"-rw-rw-r-- 1000   1000            1,200 Sep 16 15:36 SERVER.crt\" threadId=\"l2izo4YsQt6vGIm2NtYcBw\""
    - "2020/09/24 17:46:58      Result: \"Success\""
    - "2020/09/24 17:46:58      SSH FTP: \"quit()\""
    - 2020/09/24 17:46:58   End
```

If you do not specify a `username` or `connection`, the utility will search for all actions with the alias described by `action`. You can also search for actions matching filter criteria using `actionfilter` instead of `action`. If multiple actions are found, they will *all* be run. You can use `operation: list` to preview the actions that will be run.

```
- operation: run
  actionfilter: alias sw "d"
```

Will find all actions, on any user or connection, whose alias starts with `d`, and will run them all.

You can provide two additional request options to control the running of the action(s) as described in the [API reference](https://developer.cleo.com/api/api-reference/post-actions-actionid-run.html):

```
- operation: run
  connection: mysftp
  action: dir
  timeout: 300
  messagesCount: 100
```

#### Managing actions

In addition to the `run` operation, the requests described above for actions can also be used to `add`, `list`, `update` and `delete` actions directly (these operations applied to the parent object also provide a mechanism for actions to be listed, updated, and deleted in a more constrained request context).


### [&lt;](#-action-handling-) Certificate Handling [&gt;](#-multiple-profiles-)

Like actions, certificates in the native Harmony API are handled as a separate linked resource. The batch utility masks this separation by embedding certificates directly into the properties for a connection that includes certificates.

For example, the [API specification](https://developer.cleo.com/api/api-reference/post-connections-as2.html) for an AS2 connection includes:

Name | Type | Description
-----|------|------------
`partnerEncryptionCert`|object|A certificate. 
`partnerEncryptionCert.href`|string (regex: ^.*/.*$)|The URI of the certificate.
`partnerSigningCert`|object|A certificate.
`partnerSigningCert.href`|string (regex: ^.*/.*$)|The URI of the certificate.

meaning that a request to include certificates in an add request should look like:

```
---
operation: add
connection: sample
type: as2
...
  partnerEncryptionCert:
    href: /api/certs/68d7b56581a78f943539a02a9a31f603667d28da
  partnerSigningCert:
    href: /api/certs/ee203220b067f824f63384b297d0237e64651cff
...
```

where the certificates should have been imported ahead of time to obtain `href` links. The batch utility handles certificates as if they are directly embedded properties of the connection:

```
---
operation: add
connection: sample
type: as2
...
  partnerEncryptionCert:
    certificate: |-
      -----BEGIN CERTIFICATE-----
      MIIDRjCCAi6gAwIBAgIQFilIVuBNTraKlA3WHvvmuDANBgkqhkiG9w0BAQsFADAy
      MQswCQYDVQQGEwJVUzENMAsGA1UECgwEQ2xlbzEUMBIGA1UEAwwLRGVtbyBJc3N1
      ZXIwHhcNMjAxMDAxMDMyMDA4WhcNMjExMDAxMDMyMDA4WjAjMQswCQYDVQQGEwJV
      UzEUMBIGA1UEAwwLRW5jcnlwdCAxMDUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw
      ggEKAoIBAQDRCkqE7l87RJ6f4kOIDgqpPzXX3YGrXrBmxzKqQ+4Ve1hrDbuMkWDJ
      Fhfo/FwWJgvfpasbHpgNF9gP/N8pTzR7NVOa4ZPujCIWf5dnA+DH/wK5ER/zMXFb
      uCEm6ov+5WUDBxI/gxorBPUD0pPOZv8ST2NBX+jd1Xg280FJ/eWeDOGQRnaS7PGs
      Ud74LtJyTZPRWEHKglxEFcC46uButasqKPEqrLVfC4kU5Hu560DfeVwpxoL4mani
      b/pW/d/bkETUhE3XurxsT41ZxAHfAoIV7ECVHfSbIVKCJXIRhjlFtsdiYtpX/YXg
      KnBj3XpalySQEpGc3ps4yzQgnRmyjgB3AgMBAAGjZzBlMAsGA1UdDwQEAwIFoDAW
      BgNVHSUBAf8EDDAKBggrBgEFBQcDATAdBgNVHQ4EFgQUG+qvK69+NVHWdfFB10kW
      vtNGUMUwHwYDVR0jBBgwFoAU52GI/XKU1F8qd36/Z06p+mp4t9MwDQYJKoZIhvcN
      AQELBQADggEBADE2do/HqSFBzSkZHyFi2z4VgGVJq/TnG61Kdl5Kz2dfLRu1+NeW
      XmBgge5ebInza5D2+uVmMf2/G9Ws4WLelxr1yESmhMoliA6jZAyhn81/AaznNjvy
      zyTsqcFvrm6UBGNjjU3BWQMrhA6p1bcoCCuy/CSLeHJ1v+ofG1ih+31Vbq77h/ni
      w+sZjfIA3rwo9oazlC9mQdoPOGxSFT2j+ygfHKoHCLIhBkiRhcXVne4Rozof/fma
      jIhLSh/5Feu24TpdIy9vn8P/PvefRGIOu61D1Jlffc93m3oi6bXBo9JvoA+v/pJP
      2eV0+LxehsQ9CvLyvBAP8H2uH6g/y5Fl9/4=
      -----END CERTIFICATE-----
  partnerSigningCert:
    certificate: |-
      MIIDQzCCAiugAwIBAgIQLGG6JRAsQ4S5W1ZBieixJTANBgkqhkiG9w0BAQsFADAy
      MQswCQYDVQQGEwJVUzENMAsGA1UECgwEQ2xlbzEUMBIGA1UEAwwLRGVtbyBJc3N1
      ZXIwHhcNMjAxMDAxMDMyMDA4WhcNMjExMDAxMDMyMDA4WjAgMQswCQYDVQQGEwJV
      UzERMA8GA1UEAwwIU2lnbiAxMDUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
      AoIBAQC2MqK1Z1Bg08tJdpo3S6iQ7vcsBFlK4OKdkYuhf9Wioy/J+oHmichzPxE5
      jOCC9gM1iDZ9X7reEUVDKyDb83wT2qj5cO0vw7jj8hVrmybRsJLRheYRsjC5HUyR
      gIU8rG5drkwbE0UDZXYSp41puotpsGwwnctdwczNolBiSlJnv844uGtawstOE7Su
      eWG8STWLDFcdx26lo45pllpbvE0u8t6MFzwpt8z5GSzjz5wksIANg1IcIruIdmvm
      f9CZ/qS8VpFwqvsPhWdXYZqjRquo4UMmTDA26IQOn+9jQEQ0toZn7AZPS4mU5v+X
      tbzHCMq7QbMKKe2i8SvsrbKoAnTVAgMBAAGjZzBlMAsGA1UdDwQEAwIFoDAWBgNV
      HSUBAf8EDDAKBggrBgEFBQcDATAdBgNVHQ4EFgQUeBrHJ1fNFMBlzZhmRPFtbVq1
      JAUwHwYDVR0jBBgwFoAU52GI/XKU1F8qd36/Z06p+mp4t9MwDQYJKoZIhvcNAQEL
      BQADggEBAEEIXPAysj6SsibGIPH0VWeADr0w5WvsxjqnLeCXLMwvsRPUKvUPPFGB
      KgfTHcBllZl7GriylJAnPy5FpHBgXxiTp6nn8had3yM6gA8sOjG4DntNhy/Tsh96
      KpUTeP63pMj6mhLfzAuWzEQLmIgQX88FIraXWESrmZcYnZy9sS/DPnMhtwkmGYxl
      UdgcTDbUUk7Pn5wAdNiNv7swFu1ig3SYgp21opqmBtEHmbOQranJjC+nFgejyrdt
      qJpNW5gIixoslRlr8OLnU3uAwiNBQgIZHSsnjybALw3bv+ChfEAGBPfVIXtCPETZ
      9OjeQgulu5t1XepHst0rnzk9N1BWH+0=
...
```

The `certificate` property is a string, and the batch utility will accept several forms:

* a single base64-encoded string all on one line `certificate: MIIDQzCCAiugAwIBAgIQLGG6JR...k9N1BWH+0=`
* a multi-line base64-encoded string, as illustrated above for `partnerSigningCert`
* a multi-line base64-encoded string framed in `BEGIN` and `END` delimiters, as illustrated above for `partnerEncryptionCert` (this is the typical `.crt` or PEM format used by tools such as `openssl`)
* a list of multi-line base64-encoded strings framed in `BEGIN` and `END` delimiters (this is sometimes used to represent a certificate and its chain of issuers)

The certificate may be a single stand-alone certificate, represented in PEM format as:

```
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
```

or a list of PEM certificates:

```
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
```

or it may be part of a PKCS#7 certificate chain, typically found in file names ending with extensions like `.p7b` or `.p7c` or represented in PEM format as:

```
-----BEGIN PKCS7-----
...
-----END PKCS7-----
```

When processing a PKCS#7 certificate chain or a list of PEM certificates, the final "end entity" certificate is used (if for some reason there are multiple "end entity" certificates in the bundle, the first one is used) as the certificate that is linked to the connection. Any remaining certificates are imported into the certificate manager, typically the issuer chain, but are not referenced in the connection.

If the certificate is already imported into Harmony, the existing certificate "href" reference is reused and an additional cross-reference is added to the certificate's `usage` links (see [`GET certs/{certid}`](https://developer.cleo.com/api/api-reference/get-certs-certid.html)).

> **Note** only public key certificate usage contexts are supported (e.g. `partnerEncryptionCert` and `partnerSigningCert` for AS2), not private key contexts (e.g. `localEncryptionCert` and `localSigningCert` for AS2).


## [&LessLess;](#-request-processing-) Multiple Profiles [&GreaterGreater;](#-csv-files-and-templates-)

In the typical mode of operation, all requests are sent to a single Harmony (or VLTrader) server, identified by the selected connection profile (`--profile` or `default` for command line, or the first enabled entry in the Profiles table for the connector). But when needed, each request can be routed to a different Harmony/VLTrader server by including a `profile` name in each request.

### Command Line

If a profile name is provided in a request, it is matched against a profile of the same name stored in `$HOME/.cic/profiles`.

If a profile name is not provided in a request, the default profile for the command line is used. This is:

* the unnamed profile comprised of explicit command line arguments `--url`, `--username`, `--password`, and `--insecure`, if supplied, otherwise
* the profile named in the `--profile` command line option, otherwise
* the profile named `default` in `$HOME/.cic/profiles`.

### Harmony Connector

If a profile is provided in a request, it is matched against an _enabled_ profile of the same name in the `Profiles` table configured for the connector. Since the Harmony UI does not prevent multiple profiles of the same name being entered and enabled, the first matching profile is used.

If a profile is not provided in a request, a default profile from the `Profiles` table configured for the connector is selected as follows:

* the first enabled profile with a blank name, if one exists, otherwise
* the first enabled profile named `default`, if one exists, otherwise
* the first enabled profile

If a named or default profile cannot be found, the request fails with an error.

## [&LessLess;](#-multiple-profiles-) CSV Files and Templates [&GreaterGreater;](#-formatting-results)

In many cases involving batch operations, most parts of each request, or at least the request skeleton, are the same.
The detais for each request can then conveniently be represented in tabular form.

The batch utility supports this mode of operation using CSV files for the tabular data and templates in YAML format.
The CSV file must have a header, which defines replacement token names for the columns of the table.
The YAML template encodes requests, much as illustrated above, but with replacement tokens that fill in values from the CSV file, using the replacement token names from the CSV header.

For example, a simple `add` user request:

```
---
- username: alice
  password: password
  email: alice@cleo.demo
  authenticator: Users
```

could be generalized with replacement tokens for username, password, and email with a CSV file:

```
user,pass,email
alice,password,alice@cleo.demo
bob,password,bob@cleo.demo
```

against the template:

```
---
- username: ${user}
  password: ${pass}
  email: ${email}
  authenticator: Users
```

resulting in the same effect as the following explicit YAML request file:

```
---
- username: alice
  password: password
  email: alice@cleo.demo
  authenticator: Users
- username: bob
  password: password
  email: bob@cleo.demo
  authenticator: Users
```

If your column headings are not legal JavaScript identifiers see [below](#token-replacement).

### Built-in templates [&gt;](#-advanced-template-features-)

If you provide a CSV file for `--input` and do not provide an explicit `--template`, the batch utility will attempt to use one of its built-in templates based on an analysis of the content:

* files with a `UserAlias` column in the header use the `authenticator` template
* files without a `type` column in the header use the `user` template
* files with a `type` column whose data values are all `as2` use the `as2` template
* files with a `type` column whose data values are all `sftp` use the `sftp` template
* files with a `type` column whose data values are all `ftp` use the `ftp` template
* files with a `type` column whose data values are not all the same cause an error

The built-in templates support the following header columns. You may include them in your CSV file in any order, but keep in mind that the `UserAlias` and `type` columns are essential to the template selection process:

[authenticator](https://github.com/cleo/connector-batchapi/blob/master/src/main/resources/com/cleo/labs/connector/batchapi/processor/template/default/authenticator.yaml) | [user](https://github.com/cleo/connector-batchapi/blob/master/src/main/resources/com/cleo/labs/connector/batchapi/processor/template/default/user.yaml) | [as2](https://github.com/cleo/connector-batchapi/blob/master/src/main/resources/com/cleo/labs/connector/batchapi/processor/template/default/as2.yaml) | [sftp](https://github.com/cleo/connector-batchapi/blob/master/src/main/resources/com/cleo/labs/connector/batchapi/processor/template/default/sftp.yaml) | [ftp](https://github.com/cleo/connector-batchapi/blob/master/src/main/resources/com/cleo/labs/connector/batchapi/processor/template/default/ftp.yaml)
----------------|-------------------|-------------------|-------------------|----
&nbsp;          | &nbsp;            | type              | type              | type
UserAlias       | Host              | alias             | alias             | alias
&nbsp;          | &nbsp;            | url               | host              | host
&nbsp;          | &nbsp;            | &nbsp;            | port              | port
&nbsp;          | UserID            | AS2From           | username          | username
&nbsp;          | Password          | AS2To             | password          | password
FTP             | WhitelistIP       | Subject           | &nbsp;            | channelmode
SSHFTP          | &nbsp;            | encrypted         | &nbsp;            | activelowport
HTTP            | &nbsp;            | signed            | &nbsp;            | activehighport
Access          | Email             | receipt           | &nbsp;            | &nbsp;
FolderPath      | DefaultHomeDir    | receipt_sign      | &nbsp;            | &nbsp;
HomeDir         | CustomHomeDir     | receipt_type      | &nbsp;            | &nbsp;
DownloadFolder  | &nbsp;            | inbox             | inbox             | inbox
UploadFolder    | &nbsp;            | outbox            | outbox            | outbox
OtherFolder     | OtherFolder       | &nbsp;            | &nbsp;            | &nbsp;
ArchiveSent     | &nbsp;            | sentbox           | sentbox           | sentbox
ArchiveReceived | &nbsp;            | receivedbox       | receivedbox       | receivebox
&nbsp;          | CreateCollectName | CreateSendName    | CreateSendName    | CreateSendName
&nbsp;          | ActionCollect     | ActionSend        | ActionSend        | ActionSend
&nbsp;          | Schedule_Send     | Schedule_Send     | Schedule_Send     | Schedule_Send
&nbsp;          | CreateReceiveName | CreateReceiveName | CreateReceiveName | CreateReceiveName
&nbsp;          | ActionReceive     | ActionReceive     | ActionReceive     | ActionReceive
&nbsp;          | Schedule_Receive  | Schedule_Receive  | Schedule_Receive  | Schedule_Receive
&nbsp;          | action&lowbar;<i>alias</i>&lowbar;name     |action&lowbar;<i>alias</i>&lowbar;name | action&lowbar;<i>alias</i>&lowbar;name | action&lowbar;<i>alias</i>&lowbar;name
&nbsp;          | action&lowbar;<i>alias</i>&lowbar;commands | action&lowbar;<i>alias</i>&lowbar;commands | action&lowbar;<i>alias</i>&lowbar;commands | action&lowbar;<i>alias</i>&lowbar;commands
&nbsp;          | action&lowbar;<i>alias</i>&lowbar;schedule |action&lowbar;<i>alias</i>&lowbar;schedule |action&lowbar;<i>alias</i>&lowbar;schedule | action&lowbar;<i>alias</i>&lowbar;schedule
&nbsp;          | HostNotes         | &nbsp;            | &nbsp;            | &nbsp;

The user and connection templates provide fixed slots for two actions. The action&lowbar;<i>alias</i>&lowbar;xxx columns allow for an arbitrary number of additional actions to be defined. The action alias is taken from the `name` column: the _alias_ portion in the column name can be the same alias, but is really used only to match up the `name`, `commands` and `schedule` columns for the action (and to keep the sets of columns for additional actions distinct from each other&mdash;you may not have multiple columns with the same header name).

A few columns can be multi-valued:

* `WhitelistIP`: multiple IP addresses separated by `;`
* `OtherFolder`: multiple custom folder paths separated by `;`
* action&lowbar;<i>alias</i>&lowbar;commands and other action command script columns: multiple commands separated by `;` or `|`

As a convenience action&lowbar;<i>alias</i>&lowbar;schedule and other action schedule columns accept the shorthand `polling` for the official API schedule `on file continuously`.

Built-in templates can be used for only a single object type per file and a single request per row (although the user template will also automatically create the authenticator indicated in the `Host` column if it does not yet exist&mdash;see [the template](https://github.com/cleo/connector-batchapi/blob/master/src/main/resources/com/cleo/labs/connector/batchapi/processor/template/default/user.yaml)). You can construct your own templates that can use conditionals and token expressions to create multiple object types from a single CSV, or can create multiple requests per row.

### [&lt;](built-in-templates-) Advanced template features [&gt;](-formatting-results)

#### JSON and YAML

Keep in mind that the Harmony API operates on JSON data. JSON builds three simple concepts into arbitrarily rich data structures. You can think of a JSON data structure as a tree-structured collection of nodes:

* scalar nodes&mdash;strings, numbers (integers and floating point), booleans (true/false) and `null`:<br/>`'string'` or `"string"`, `25`, `3.14`, `true`, `false`, `null`, ...
* array nodes&mdash;lists of other nodes, including scalars, objects, and other arrays:<br/>`[node, node, node, ...]`
* object nodes&mdash;sets of named fields, where the names are strings and the values are other nodes, including scalars, arrays, and other objects:<br/>`{"name":node,"name":node,"name":node,...}`

YAML provides an optional alternate representation for arrays and objects, which replaces the `[]` and `{}` syntax with indentation (it also makes the `'` and `"` enclosing strings optional is most cases). Arrays are represented as nodes introduced by `-` at the same indentation level. Objects are represented by `name:` at the same indentation level. Nested arrays and objects are indicated by increasing indenting (by spaces&mdash;tabs are prohibited in YAML) instead of potentially confusing groups of e.g. `{{[{}]}}`.

YAML arrays `[1,2,3]` and `[{"user":"bob","age":42},{"user":"amy","special":true,"details":{"reason":"happy"}}]`:

```
- 1
- 2
- 3
```

```
- user: bob
  age: 42
- user: amy
  special: true
  details:
    reason: happy
```

#### Token replacement

The most basic template feature is token replacement. Any field name or scalar value in the tree is scanned for `${token}` blocks, and these are replaced by the corresponding token (or nothing, if the token is not defined or its cell is empty in the CSV file). Multiple `${token}` may appear in a single field name or value. So if `a` is `lions` and `b` is `lambs`:

> `This example shows ${a} and ${b} together` &rarr; `This example shows lions and lambs together`

It is possible for a CSV header to include column names that are not legal JavaScript variable names, for example `Yes/No` or `ID#`. In this case you cannot refer to the token directly:

> `${Yes/No}` &rarr; error!

and instead you must enclose the column name in `column['column name']`:

> `${column['Yes/No']}` &rarr; `Yes` or `No` depending on the column value

#### JavaScript expressions

In fact the `token` is more accurately described as a JavaScript expression. Every column value is converted into a JavaScript variable (whose name is taken from the column header), so `${a}` is really just evaluating the `a` variable in the JavaScript engine. But other expressions may be used as needed:

> `${a.toUpperCase()}` &rarr; `LIONS`
>
> `${a.length}` &rarr; `5`

Whenever an `${expression}` appears with other text or tokens, it is converted into a string. If an `${expression}` appears all by itself in a value as a _singleton_, in some cases it is necessary to make sure it is rendered as a scalar of a specific type. This can be achieved by appending `:int`, `:boolean` or `:string` to force the appropriate interpretation.

Note that there are places in the Harmony API where `true` or `false` is a boolean, while in other places it must be quoted as a string `"true"` or `"false"` (because the property in question may take other possible values, so it is modeled in the API as a String). Use `:boolean` whenever the property is a true boolean&mdash;otherwise it will be evaluated as a string.

#### Manipulating tree structure

##### Conditionals: `${if:`, `${else if:` and `${else}`

The template expander supports _conditional_ expressions, evaluating a JavaScript expression before expanding a portion of the template. For conditional expansion use the special `${if:expression}` singleton as a field name in the template. If the expression evaluates to a value that is considered true-ish, the value attached to that special field is merged into the parent context.

For `${if` the following are considered true-ish:

* a boolean value of true
* any non-empty string other than `no`, `none`, `na` or `false` (case insensitive)
* any non-zero integer (or non-0.0 floating point number)
* JavaScript `null` is considered `false`

If the condition is satisfied, the value could be any of scalar, object, or array. How this value is merged depends on the parent context of the `${if` field:

* if the `${if` was an element of a parent array, a scalar or object value is added to the parent array, and all elements of any array value are added as (possibly) multiple entries in the parent array (a nested array is _not_ created&mdash;if this is what you need you must wrap the result array in another array).
* if the `${if` was a field of an object, the value _must_ be an object, and that objects fields are merged into the parent object. Array and scalar values lack any kind of field name under which to merge the values into the parent.

An array example:

```
---
- ${if:true}:
  - 1
  - 2
- ${if:true}: 3
- ${if:false}: 4
- ${if:true}:
    result: 5
- 6
```

produces

```
---
- 1
- 2
- 3
- result: 5
- 6
```

An object example:

```
---
${if:true}:
  field1: 1
  field2: 2
${if again:true}:
  field3: 3
fixed4: 4
${if:false}:
  result: 5
fixed6: 6
```

produces:

```
---
field1: 1
field2: 2
field3: 3
fixed4: 4
fixed6: 6
```

Notice that the previous example uses `${if:true}` and `${if again:true}`. Since the template is converted to a JSON object whose keys must be unique, two `${if:true}` at the same level is invalid (or more precisely, the second one overwrites the first and you get an unintended result). To allow you to make your conditional expressions unique, you may add additional text between the `if` or `else` and the `:` (or `}` for just plain `${else}`, which is where this potential problem is mostly likely to arise).

After a `${if:}` expression you can follow with additional `${if:` expressions, or `${else if:` or `${else}` expressions. `${else if:` and `${else}` must appear at the same level in the template as a preceding `${if:`.
It is permitted to nest `${if:`/`${else if:`/`${else}` sequences deeper in the template.

##### Loops: `${for:`

The template expander supports looping over template fragments based on either:

* splitting a column value, typically on some separator (see `WhitelistIP` or action commands)
* matching multiple column headers, for example the action&lowbar;<i>alias</i>&lowbar;xxx columns

Like a conditional, a loop is indicated by a special singleton expression in a field name:

* `${for identifier:expression}` to loop over an expression using column values
* `${for column identifier:regex}` to loop over column names matching `regex`

Processing is similar to a conditional, except that the value node of the `${for:` field is evaluated once for every value of the `expression` or every match of the `regex`. In these evaluations, a new JavaScript variable is injected into the JavaScript engine using the selected `identifier` (so make sure the chosen `identifier` does not mask one of your column headings). Merging of the results follows the same rules as for conditional values.

The `expression` should be an array expression: the most usual case is something like `${for id:value.split(/;/)}` to separate a multi-valued column value into its parts. Constant arrays can also be used like `${for id:[1,2,3]}`.

The `regex` may have a capture group to map a matching subportion of the column name to the bound `identifier`. If no capture groups are defined, the whole column name is used. Only the first capture group is used. For example, the action&lowbar;<i>alias</i>&lowbar;xxx columns are processed in the built-in templates as follows:

```
- connection: ${alias}
  ... lots of template goes here ...
  actions:
    ${for column action:action_([^_]+)_alias}:
      ${eval('action_'+action+'_alias')}:
        alias: ${eval('action_'+action+'_alias')}
        commands:
        - ${for command:eval('action_'+action+'_commands').split(/;\|/)}: ${command}
        schedule: ${s=eval('action_'+action+'_schedule');s=='polling'?'on file continuously':s}
```

##### Null pruning

As with conditionals, any object field whose value evaluates to `null` is omitted from the template expansion. Any array that then ends up empty or object that ends up with no fields is also omitted. These omissions can propagate up the template tree to omit entire branches if the leaf nodes are not expanded.

For example, in this fragment from the built-in user template:

```
---
- username: ${username}
  ...
  home:
    subfolders:
      default:
      - ${for other:OtherFolder.split(';')}:
          usage: other
          path: ${other}
```

the entire `subfolders` branch will be pruned if `OtherFolder` is blank or null. But in this example from the authenticator template:

```
---
type: nativeUser
...
  home:
  subfolders:
    default:
    - ${if:DownloadFolder}:
        usage: download
        path: ${DownloadFolder}
```

the `usage: download` field will prevent the entry in the `default:` array from being pruned, which will then prevent the entire `default:` array and possibly the `subfolders:` field from being pruned. So the `${if:DownloadFolder}:` conditional is needed to force the needed pruning.

#### Built-in Functions

In addition to the standard JavaScript environment, the following functions and variables are built-in to the template expansion facility:

##### `date(format)`

Expands to a timestamp of the current time (as of when template processing started&mdash;all `date` functions in a template will expand using exactly the same time instanc) using a Java [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html). Examples from the Java documentation:

> The following examples show how date and time patterns are interpreted in the U.S. locale. The given date and time are 2001-07-04 12:08:56 local time in the U.S. Pacific Time time zone.
> 
> Date and Time Pattern          | Result
> -------------------------------|-------
> "yyyy.MM.dd G 'at' HH:mm:ss z" | 2001.07.04 AD at 12:08:56 PDT
> "EEE, MMM d, ''yy"             | Wed, Jul 4, '01
> "h:mm a"                       | 12:08 PM
> "hh 'o''clock' a, zzzz"        | 12 o'clock PM, Pacific Daylight Time
> "K:mm a, z"                    | 0:08 PM, PDT
> "yyyyy.MMMMM.dd GGG hh:mm aaa" | 02001.July.04 AD 12:08 PM
> "EEE, d MMM yyyy HH:mm:ss Z"   | Wed, 4 Jul 2001 12:08:56 -0700
> "yyMMddHHmmssZ"                | 010704120856-0700
> "yyyy-MM-dd'T'HH:mm:ss.SSSZ"   | 2001-07-04T12:08:56.235-0700
> "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" | 2001-07-04T12:08:56.235-07:00
> "YYYY-'W'ww-u"                 | 2001-W27-3

##### `generatePassword()`

Expands to a new password according to the format described above at [Password Generation](#-password-generation-). This function provides more fine-grained control than the `--generate-pass` option, which overrides any fixed passwords that may otherwise appear in the template (and will insert a password in `accept.password` if one is not supplied). When using `generatePassword()` for `accept.password`, generated and fixed (or otherwise calculated) passwords may be freely intermingled.

Keep in mind that a password is _required_ when adding users.

##### `exists(type, name)` or `exists(type, name, profile)`

Expands to a `true` if the named resource of the specified type exists (in the Harmony indicated by the default or the named profile), or `false` otherwise. The following `type`s are understood:

Type            | Description
----------------|------------
"user"          | if `name` is formatted as `"authenticator\name"`, then an attempt to find user `name` under authenticator `authenticator` is made. If no `authenticator` is provided (just `"name"`), then all authenticators are searched for user `name`. User names are required to be unique across authenticators, but if the authenticator name can be supplied the search is much more efficient.
"authenticator" | searches for the named `authenticator`, of any type (not just `nativeUser`).
"connection"    | searches for the named `connection`, of any type.

Unlike the other built-in functions, `exists` requires a live API connection. This is indicated by the (optional) profile name and profile selection is handled as described in [Multiple Profiles](#-multiple-profiles-).

When `--operation preview` is in effect, `exists()` always returns `false`.

#### Testing your template

Use `--operation preview` to test the effects of your template on your CSV data. No API calls will be made to the Harmony API, but the requests will be converted to YAML and displayed directly in the result output with the message "request preview".

## [&LessLess;](#-csv-files-and-templates-) Formatting Results

By default the results of processing the requests in the input file(s) is(are) reported in a YAML format, as illustrated in several examples above. Some additional options allow this format to be altered.

If JSON output is preferred to YAML, use `--output-format json` on the command line or select `Output Format: json` in the connector configuration. JSON output is structurally identical to YAML&mdash;the syntax is just changed to use only valid JSON constructs. The JSON is indented ("pretty printed") for easier reading by humans in a fashion that does not affect automated processing by programs.

CSV output is not structurally equivalent to YAML and JSON, so an additional processing step is required to "flatten" the results into a row/column tabular format suitable for output in CSV. This "flattening" process is controlled by an output template, using the same expressions and features of the templates used to process CSV input. But the output template must be "flat": a simple object whose field names correspond to CSV columns and whose values are simple values (strings, booleans, numbers) and not nested objects or arrays.

While the source for mapping input CSV files is a set of columns, referenced in the template as `${column['column name']}` (or `${column name}` for suitably named columns), the source for the output CSV template is the result object named simply `${data}`. For example, a simple template to report on added users might be:

```
---
user: ${data.username}
password: ${data.accept.password}
email: ${data.email}
group: {$data.authenticator}
```

As with input templates, values that are missing or null are skipped.

The resulting CSV file will include a column heading line whose labels match the field names in the template (enclose the field names in quotes if they are not valid JavaScript identifiers). The order of the columns will be the order in which values are discovered in the output, and the column types will be inferred from the content found.

To provide more control over the column headers, you may include a list of columns at the beginning of the template. This will force columns in the specified order in the output schema, even if no entries produce values for those columns. To define columns, include a `columns` field in the template, followed by a `template` field encapsulating the actual template. For example:

```
---
columns:
- name: user
- name: email
- name: password
- name: group
- name: extra
template:
- ${if:data.username}:
    user: ${data.username}
    email: ${data.email}
    password: ${data.accept.password}
    group: ${data.authenticator}
    ${if:data.accept.sftp.key}:
      sftpkey: true
```

will include the `user`, `email`, `password`, `group` and `extra` columns, whether they have values or not. The `sftpkey` column will be added if any results are present with that field (those for which `data.accept.sftp.key` has a value).

Additionally, this template produces an output line only if `data.username` is present, skipping over result entries of other types. This could be useful, for example, with a list operation on an authenticator (group), which includes the authenticator (skipped) and all its associated users (included) in the output.

Notice also that this template is structured as an array (although there is only one entry in the array). If a template includes multiple array entries that produce output, this will result in multiple lines being produced in the CSV output.

Errors that occur during processing of the request will have corresponding error results. They can be mapped into the CSV output or not, depending on the requirements (`${data.result.status}` will be `'error'`). Any errors that occur during the "flattening" of results into rows for the CSV output will be appended to the CSV file as text. You can use `${error}` to propagate error results to CSV errors with a construct like:

```
---
${if:data.result.status=='error'}:
  ${error}: ${data.result.message}
...
```

If the input file was a CSV file (if `--template` or `Template` was specified), the original parsed CSV data is included in the `data.result.csvdata` object. This may be useful producing an output format that reflects the input when some input columns are not directly reflected in the created objects.

```
---
columns:
- name: Ignored Column
- name: Username
tenmplate:
  Ignored Column: ${data.result.csvdata["Original Input"]}
  Username: ${data.username}
```

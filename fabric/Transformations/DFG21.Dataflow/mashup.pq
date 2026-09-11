[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared customers = let
  Source = FabricSql.Contents(null),
  Navigation = Source{[workspaceId = "a53675cc-050d-4846-abbc-c5e719e82670"]}[Data],
  #"Navigation 1" = Navigation{[sqlId = "2d58dcff-6633-4129-9316-e0bad8a67026"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "customers"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([City] = "Berlin" or [City] = "Cologne"))
in
  #"Filtered rows";
shared city_information = let
  Source = FabricSql.Contents(null),
  Navigation = Source{[workspaceId = "a53675cc-050d-4846-abbc-c5e719e82670"]}[Data],
  #"Navigation 1" = Navigation{[sqlId = "2d58dcff-6633-4129-9316-e0bad8a67026"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "cities_info"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(customers, {"City"}, city_information, {"city"}, "customers (2)", JoinKind.LeftOuter),
  #"Expanded customers (2)" = Table.ExpandTableColumn(Source, "customers (2)", {"info"}, {"info"})
in
  #"Expanded customers (2)";
shared Merge_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "a53675cc-050d-4846-abbc-c5e719e82670"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ff3b968d-8c21-4645-9e47-6366b4449df7"]}[Data],
  Navigation_3 = Navigation_2{[Id = "staging", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dfg2", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

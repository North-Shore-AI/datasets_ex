defmodule DatasetsExTest do
  use ExUnit.Case
  doctest DatasetsEx

  test "module loads correctly" do
    assert Code.ensure_loaded?(DatasetsEx)
    assert Code.ensure_loaded?(DatasetsEx.Dataset)
    assert Code.ensure_loaded?(DatasetsEx.Loader)
  end
end

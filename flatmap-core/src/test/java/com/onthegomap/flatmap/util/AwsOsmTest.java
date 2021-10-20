package com.onthegomap.flatmap.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class AwsOsmTest {

  private static final byte[] response = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>osm-pds</Name>
      <Prefix/>
      <Marker/>
      <MaxKeys>1000</MaxKeys>
      <IsTruncated>false</IsTruncated>
      <Contents>
        <Key>2021/planet-210830.orc</Key>
        <LastModified>2021-09-05T00:53:27.000Z</LastModified>
        <ETag>"3e38c5a4c1db83a70abaafb3e6784d51-818"</ETag>
        <Size>85742946714</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210830.osm.pbf</Key>
        <LastModified>2021-09-04T13:02:46.000Z</LastModified>
        <ETag>"4373b6cd606f6c6f7d357e3b3c77ba6f-7632"</ETag>
        <Size>64018213652</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210830.osm.pbf.md5</Key>
        <LastModified>2021-09-04T13:00:10.000Z</LastModified>
        <ETag>"6555b62666e14a24323d13b34126e5b3"</ETag>
        <Size>56</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210830.osm.pbf.torrent</Key>
        <LastModified>2021-09-04T13:02:46.000Z</LastModified>
        <ETag>"b2b82d568dec00f5e361358530672f25"</ETag>
        <Size>306696</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210830.osm.pbf.torrent.md5</Key>
        <LastModified>2021-09-04T13:00:10.000Z</LastModified>
        <ETag>"5e0eaf44e8a69e7cac706d0a052a5356"</ETag>
        <Size>277</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210906.orc</Key>
        <LastModified>2021-09-12T00:35:53.000Z</LastModified>
        <ETag>"a89c489c799019c5e5f914ef5ba5c030-820"</ETag>
        <Size>85890640680</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210906.osm.pbf</Key>
        <LastModified>2021-09-11T13:47:44.000Z</LastModified>
        <ETag>"c0c1a0ffdf1dd6ece9915bc7a568dfd3-7645"</ETag>
        <Size>64122989442</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210906.osm.pbf.md5</Key>
        <LastModified>2021-09-11T13:45:10.000Z</LastModified>
        <ETag>"738b54c550a99704b47b18e7afe287dc"</ETag>
        <Size>56</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210906.osm.pbf.torrent</Key>
        <LastModified>2021-09-11T13:47:44.000Z</LastModified>
        <ETag>"c734185715f555666acff7fa3d1dd504"</ETag>
        <Size>307196</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2021/planet-210906.osm.pbf.torrent.md5</Key>
        <LastModified>2021-09-11T13:45:10.000Z</LastModified>
        <ETag>"5e0eaf44e8a69e7cac706d0a052a5356"</ETag>
        <Size>277</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>changesets/changesets-latest.orc</Key>
        <LastModified>2021-09-11T13:02:24.000Z</LastModified>
        <ETag>"a63ce01a6dd033c94c862a690f10c9e3-468"</ETag>
        <Size>3917489634</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>planet-history/history-latest.orc</Key>
        <LastModified>2021-09-12T21:17:59.000Z</LastModified>
        <ETag>"322389f0eb7dd3fe3d597c9383fee4af-8620"</ETag>
        <Size>144609225892</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>planet/planet-latest.orc</Key>
        <LastModified>2021-09-12T11:08:29.000Z</LastModified>
        <ETag>"4ce8adc7208a0423cd4bb48ca087bf76-5120"</ETag>
        <Size>85890640680</Size>
        <Owner>
          <ID>3555355acefe4c7705bd24eb6def06f2564b97fa9b5fe5bc435443a21476f67d</ID>
          <DisplayName>seth+osmpds</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
    </ListBucketResult>
    """.getBytes(StandardCharsets.UTF_8);

  @Test
  public void testFound() throws IOException {
    var index = AwsOsm.parseIndexXml(new ByteArrayInputStream(response));
    assertEquals("https://osm-pds.s3.amazonaws.com/2021/planet-210906.osm.pbf",
      AwsOsm.searchIndexForDownloadUrl("210906", index));
    assertEquals("https://osm-pds.s3.amazonaws.com/2021/planet-210830.osm.pbf",
      AwsOsm.searchIndexForDownloadUrl("210830", index));
  }

  @Test
  public void testLatest() throws IOException {
    var index = AwsOsm.parseIndexXml(new ByteArrayInputStream(response));
    String url = AwsOsm.searchIndexForDownloadUrl("latest", index);
    assertEquals("https://osm-pds.s3.amazonaws.com/2021/planet-210906.osm.pbf", url);
  }

  @Test
  public void testNotFound() throws IOException {
    var index = AwsOsm.parseIndexXml(new ByteArrayInputStream(response));
    assertThrows(IllegalArgumentException.class,
      () -> AwsOsm.searchIndexForDownloadUrl("1231", index));
  }
}

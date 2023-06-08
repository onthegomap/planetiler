package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AwsOsmTest {

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

  private static final byte[] overtureResponse =
    """
      <?xml version="1.0" encoding="UTF-8"?>
      <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
         <Name>overturemaps-us-west-2</Name>
         <Prefix />
         <Marker />
         <MaxKeys>1000</MaxKeys>
         <IsTruncated>false</IsTruncated>
         <Contents>
            <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-cook_county_il.geojsonseq.gz</Key>
            <LastModified>2023-04-13T16:47:32.000Z</LastModified>
            <ETag>"89e67e64e866c56db9abe700652bd253-17"</ETag>
            <Size>138598884</Size>
            <Owner>
               <ID>15b7f69019aeb60070a7e5e0abef900903bcad93b2cd252fc9ac14881f1cea14</ID>
               <DisplayName>sethfitz+overture-data-distribution</DisplayName>
            </Owner>
            <StorageClass>INTELLIGENT_TIERING</StorageClass>
         </Contents>
         <Contents>
            <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-eastern_ma.geojsonseq.gz</Key>
            <LastModified>2023-04-13T16:47:57.000Z</LastModified>
            <ETag>"983fbf7d59ea77478851059edf20ae82-17"</ETag>
            <Size>139513514</Size>
            <Owner>
               <ID>15b7f69019aeb60070a7e5e0abef900903bcad93b2cd252fc9ac14881f1cea14</ID>
               <DisplayName>sethfitz+overture-data-distribution</DisplayName>
            </Owner>
            <StorageClass>INTELLIGENT_TIERING</StorageClass>
         </Contents>
         <Contents>
            <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-king_county_wa.geojsonseq.gz</Key>
            <LastModified>2023-04-13T16:48:49.000Z</LastModified>
            <ETag>"fb9b875482136a1d77b8722aa0ccdd49-8"</ETag>
            <Size>60249069</Size>
            <Owner>
               <ID>15b7f69019aeb60070a7e5e0abef900903bcad93b2cd252fc9ac14881f1cea14</ID>
               <DisplayName>sethfitz+overture-data-distribution</DisplayName>
            </Owner>
            <StorageClass>INTELLIGENT_TIERING</StorageClass>
         </Contents>
         <Contents>
            <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-maricopa_and_pinal_counties_az.geojsonseq.gz</Key>
            <LastModified>2023-04-13T16:49:12.000Z</LastModified>
            <ETag>"fd0ac4f256e5e4937abdc471486637f6-15"</ETag>
            <Size>125059646</Size>
            <Owner>
               <ID>15b7f69019aeb60070a7e5e0abef900903bcad93b2cd252fc9ac14881f1cea14</ID>
               <DisplayName>sethfitz+overture-data-distribution</DisplayName>
            </Owner>
            <StorageClass>INTELLIGENT_TIERING</StorageClass>
         </Contents>
         <Contents>
            <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-orange_county_fl.geojsonseq.gz</Key>
            <LastModified>2023-04-13T16:49:59.000Z</LastModified>
            <ETag>"3e1aa7ac82267d947daf3541f91cdb6d-4"</ETag>
            <Size>25542787</Size>
            <Owner>
               <ID>15b7f69019aeb60070a7e5e0abef900903bcad93b2cd252fc9ac14881f1cea14</ID>
               <DisplayName>sethfitz+overture-data-distribution</DisplayName>
            </Owner>
            <StorageClass>INTELLIGENT_TIERING</StorageClass>
         </Contents>
         <Contents>
            <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-santa_clara_county_ca.geojsonseq.gz</Key>
            <LastModified>2023-04-13T16:50:10.000Z</LastModified>
            <ETag>"1a104df658f2667107a5d20ac4b503f2-8"</ETag>
            <Size>64331924</Size>
            <Owner>
               <ID>15b7f69019aeb60070a7e5e0abef900903bcad93b2cd252fc9ac14881f1cea14</ID>
               <DisplayName>sethfitz+overture-data-distribution</DisplayName>
            </Owner>
            <StorageClass>INTELLIGENT_TIERING</StorageClass>
         </Contents>
         <Contents>
            <Key>release/2023-04-02-alpha/planet-2023-04-02-alpha.osm.pbf</Key>
            <LastModified>2023-04-05T16:24:04.000Z</LastModified>
            <ETag>"a45f7016445256b2735f7765c7668e87-5496"</ETag>
            <Size>92196566177</Size>
            <Owner>
               <ID>15b7f69019aeb60070a7e5e0abef900903bcad93b2cd252fc9ac14881f1cea14</ID>
               <DisplayName>sethfitz+overture-data-distribution</DisplayName>
            </Owner>
            <StorageClass>INTELLIGENT_TIERING</StorageClass>
         </Contents>
      </ListBucketResult>
      """
      .getBytes(StandardCharsets.UTF_8);

  @Test
  void testFound() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(response));
    assertEquals("https://base.url/2021/planet-210906.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("210906", index));
    assertEquals("https://base.url/2021/planet-210830.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("210830", index));
  }

  @Test
  void testLatest() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(response));
    String url = awsOsm.searchIndexForDownloadUrl("latest", index);
    assertEquals("https://base.url/2021/planet-210906.osm.pbf", url);
  }

  @Test
  void testNotFound() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(response));
    assertThrows(IllegalArgumentException.class,
      () -> awsOsm.searchIndexForDownloadUrl("1231", index));
  }

  @Test
  void testOvertureMaps() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(overtureResponse));
    assertEquals(
      "https://base.url/release/2023-04-02-alpha/planet-2023-04-02-alpha.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("latest", index)
    );
    assertEquals(
      "https://base.url/release/2023-04-02-alpha/planet-2023-04-02-alpha.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("2023-04-02-alpha", index)
    );
  }
}

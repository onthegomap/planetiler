package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AwsOsmTest {

  private static final byte[] response = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>osm-pds</Name>
      <Prefix/>
      <NextContinuationToken>nextpage</NextContinuationToken>
      <KeyCount>1000</KeyCount>
      <MaxKeys>1000</MaxKeys>
      <IsTruncated>true</IsTruncated>
      <Contents>
        <Key>2023/planet-230102.orc</Key>
        <LastModified>2023-01-08T11:34:39.000Z</LastModified>
        <ETag>"49a5e81ff968d16a0ac6e0b78c4e3303-924"</ETag>
        <Size>96802097226</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230102.osm.pbf</Key>
        <LastModified>2023-01-07T23:03:45.000Z</LastModified>
        <ETag>"9664609944bd7022aa86c6ec390f0fe0-8568"</ETag>
        <Size>71872573268</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230102.osm.pbf.md5</Key>
        <LastModified>2023-01-07T23:00:11.000Z</LastModified>
        <ETag>"a9681d16f0011d7126b99327ac6d1471"</ETag>
        <Size>56</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230102.osm.pbf.torrent</Key>
        <LastModified>2023-01-07T23:03:45.000Z</LastModified>
        <ETag>"74513fffa6e06ff7acac9cd1f59d514b"</ETag>
        <Size>344019</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230102.osm.pbf.torrent.md5</Key>
        <LastModified>2023-01-07T23:00:11.000Z</LastModified>
        <ETag>"6943a7a5dcddd146e3906e71e343e489"</ETag>
        <Size>277</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230109.orc</Key>
        <LastModified>2023-01-15T13:01:34.000Z</LastModified>
        <ETag>"42a76f8c06ec62f629afc34bbf3b09bb-925"</ETag>
        <Size>96942728676</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230109.osm.pbf</Key>
        <LastModified>2023-01-15T03:17:15.000Z</LastModified>
        <ETag>"19d28987233519932b1c1e25c8c5663f-8580"</ETag>
        <Size>71970123667</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230109.osm.pbf.md5</Key>
        <LastModified>2023-01-15T03:15:11.000Z</LastModified>
        <ETag>"c53f91a851336234467e1c11750bc1dc"</ETag>
        <Size>56</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230109.osm.pbf.torrent</Key>
        <LastModified>2023-01-15T03:17:15.000Z</LastModified>
        <ETag>"145fb95bb3e2d2b0ba38280ebddbe881"</ETag>
        <Size>344499</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230109.osm.pbf.torrent.md5</Key>
        <LastModified>2023-01-15T03:15:11.000Z</LastModified>
        <ETag>"6943a7a5dcddd146e3906e71e343e489"</ETag>
        <Size>277</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230116.orc</Key>
        <LastModified>2023-01-23T03:05:03.000Z</LastModified>
        <ETag>"e2d2c430850d6804e2d5c6e3fccfd0fa-927"</ETag>
        <Size>97157879124</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230116.osm.pbf</Key>
        <LastModified>2023-01-22T15:32:20.000Z</LastModified>
        <ETag>"27af8ae8a3614a8a6a8300a3de93991f-8599"</ETag>
        <Size>72127252191</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230116.osm.pbf.md5</Key>
        <LastModified>2023-01-22T15:30:10.000Z</LastModified>
        <ETag>"6943a7a5dcddd146e3906e71e343e489"</ETag>
        <Size>277</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230116.osm.pbf.torrent</Key>
        <LastModified>2023-01-22T16:15:23.000Z</LastModified>
        <ETag>"95e1808f844c7a657e53ceae02166ece"</ETag>
        <Size>345239</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230116.osm.pbf.torrent.md5</Key>
        <LastModified>2023-01-22T16:15:10.000Z</LastModified>
        <ETag>"6943a7a5dcddd146e3906e71e343e489"</ETag>
        <Size>277</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230123.orc</Key>
        <LastModified>2023-01-30T05:44:28.000Z</LastModified>
        <ETag>"535fabb24c948525d8a807a7f9cc9fc3-929"</ETag>
        <Size>97310366433</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230123.osm.pbf</Key>
        <LastModified>2023-01-29T13:02:20.000Z</LastModified>
        <ETag>"b871759f59e0abb925d0bf372a4acf89-8611"</ETag>
        <Size>72232247230</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230123.osm.pbf.md5</Key>
        <LastModified>2023-01-29T13:00:10.000Z</LastModified>
        <ETag>"31e1303c9c35eb043cb3da55ef082de5"</ETag>
        <Size>56</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230123.osm.pbf.torrent</Key>
        <LastModified>2023-01-29T13:02:20.000Z</LastModified>
        <ETag>"903aecd263c00856d7441bce9550a9d5"</ETag>
        <Size>345739</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230123.osm.pbf.torrent.md5</Key>
        <LastModified>2023-01-29T13:00:10.000Z</LastModified>
        <ETag>"6943a7a5dcddd146e3906e71e343e489"</ETag>
        <Size>277</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230130.orc</Key>
        <LastModified>2023-02-06T14:07:57.000Z</LastModified>
        <ETag>"dead001d3a54054e9c81d1312df62ff9-930"</ETag>
        <Size>97476386631</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <Contents>
        <Key>2023/planet-230130.osm.pbf</Key>
        <LastModified>2023-02-05T12:47:17.000Z</LastModified>
        <ETag>"5c227c0151b5d1ccae1713b0c7b63427-8625"</ETag>
        <Size>72344054646</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
    </ListBucketResult>
    """.getBytes(StandardCharsets.UTF_8);

  private static final byte[] overtureResponse =
    """
      <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>overturemaps-us-west-2</Name>
        <Prefix/>
        <KeyCount>7</KeyCount>
        <MaxKeys>1000</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Contents>
          <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-cook_county_il.geojsonseq.gz</Key>
          <LastModified>2023-04-13T16:47:32.000Z</LastModified>
          <ETag>"89e67e64e866c56db9abe700652bd253-17"</ETag>
          <Size>138598884</Size>
          <StorageClass>INTELLIGENT_TIERING</StorageClass>
        </Contents>
        <Contents>
          <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-eastern_ma.geojsonseq.gz</Key>
          <LastModified>2023-04-13T16:47:57.000Z</LastModified>
          <ETag>"983fbf7d59ea77478851059edf20ae82-17"</ETag>
          <Size>139513514</Size>
          <StorageClass>INTELLIGENT_TIERING</StorageClass>
        </Contents>
        <Contents>
          <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-king_county_wa.geojsonseq.gz</Key>
          <LastModified>2023-04-13T16:48:49.000Z</LastModified>
          <ETag>"fb9b875482136a1d77b8722aa0ccdd49-8"</ETag>
          <Size>60249069</Size>
          <StorageClass>INTELLIGENT_TIERING</StorageClass>
        </Contents>
        <Contents>
          <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-maricopa_and_pinal_counties_az.geojsonseq.gz</Key>
          <LastModified>2023-04-13T16:49:12.000Z</LastModified>
          <ETag>"fd0ac4f256e5e4937abdc471486637f6-15"</ETag>
          <Size>125059646</Size>
          <StorageClass>INTELLIGENT_TIERING</StorageClass>
        </Contents>
        <Contents>
          <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-orange_county_fl.geojsonseq.gz</Key>
          <LastModified>2023-04-13T16:49:59.000Z</LastModified>
          <ETag>"3e1aa7ac82267d947daf3541f91cdb6d-4"</ETag>
          <Size>25542787</Size>
          <StorageClass>INTELLIGENT_TIERING</StorageClass>
        </Contents>
        <Contents>
          <Key>release/2023-04-02-alpha/building-extracts/2023-04-02-alpha-santa_clara_county_ca.geojsonseq.gz</Key>
          <LastModified>2023-04-13T16:50:10.000Z</LastModified>
          <ETag>"1a104df658f2667107a5d20ac4b503f2-8"</ETag>
          <Size>64331924</Size>
          <StorageClass>INTELLIGENT_TIERING</StorageClass>
        </Contents>
        <Contents>
          <Key>release/2023-04-02-alpha/planet-2023-04-02-alpha.osm.pbf</Key>
          <LastModified>2023-04-05T16:24:04.000Z</LastModified>
          <ETag>"a45f7016445256b2735f7765c7668e87-5496"</ETag>
          <Size>92196566177</Size>
          <StorageClass>INTELLIGENT_TIERING</StorageClass>
        </Contents>
      </ListBucketResult>
      """
      .getBytes(StandardCharsets.UTF_8);

  @Test
  void testFound() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(response));
    assertEquals("nextpage", index.nextToken());
    assertTrue(index.truncated());
    assertEquals("https://base.url/2023/planet-230102.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("230102", index.contents()));
    assertEquals("https://base.url/2023/planet-230116.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("230116", index.contents()));
  }

  @Test
  void testLatest() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(response));
    String url = awsOsm.searchIndexForDownloadUrl("latest", index.contents());
    assertEquals("https://base.url/2023/planet-230130.osm.pbf", url);
  }

  @Test
  void testNotFound() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(response));
    assertThrows(IllegalArgumentException.class,
      () -> awsOsm.searchIndexForDownloadUrl("1231", index.contents()));
  }

  @Test
  void testOvertureMaps() throws IOException {
    var awsOsm = new AwsOsm("https://base.url/");
    var index = awsOsm.parseIndexXml(new ByteArrayInputStream(overtureResponse));
    assertEquals(
      "https://base.url/release/2023-04-02-alpha/planet-2023-04-02-alpha.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("latest", index.contents())
    );
    assertEquals(
      "https://base.url/release/2023-04-02-alpha/planet-2023-04-02-alpha.osm.pbf",
      awsOsm.searchIndexForDownloadUrl("2023-04-02-alpha", index.contents())
    );
  }
}
